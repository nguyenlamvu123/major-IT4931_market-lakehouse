# Batch layer & Data Lake (vai C)

Mỗi đêm đọc lại toàn bộ dữ liệu thô trong MinIO, làm sạch và tính những con số chính xác hơn
luồng realtime: giá ngày, tương quan giữa các lớp tài sản với lợi suất trái phiếu, đường cong
lợi suất và phân cụm tài sản bằng MLlib. Kết quả nằm ở MinIO (gold) và Cassandra (cho dashboard
của D). Hợp đồng dùng chung: `contracts/paths.yaml`, `contracts/cassandra.cql`,
`contracts/schemas/`.

```text
Kafka Connect (A) ──► bronze/<topic>/dt=YYYY-MM-DD/*.json
                         │  jobs/bronze_to_silver.py   (validate, dedupe, UTC)
                         ▼
                      silver/{trades,bars,yields,alerts}/dt=…   + silver/_stats/dt=…
                         │  jobs/quality_checks.py     (cổng chất lượng, lỗi → dừng DAG)
                         ▼
                         │  jobs/silver_to_gold.py     (Spark: daily, tương quan, yield curve, features)
                         ▼
                      gold/{daily_summary,correlation_matrix,yield_curve,ml_features}/dt=…
                         │  jobs/cassandra_sink.py ──► market.daily_summary / correlation_matrix / yield_curve
                         │  jobs/train_kmeans.py   ──► gold/asset_clusters + market.asset_clusters
                         ▼
                      market-api / Grafana (D)
```

## Bố cục thư mục

| File | Vai trò |
|---|---|
| `lake.py` | Đường dẫn bronze/silver/gold, tham số `--dt` / `--start --end`, kiểm tra path tồn tại |
| `schemas.py` | Schema Spark để đọc JSON bronze (có `_corrupt_record`) |
| `silver.py` | bronze → silver: kiểm tra luật hợp đồng, bỏ trùng tất định, thống kê `_stats` |
| `quality.py` | Luật chất lượng (thuần Python) + thu số liệu bằng Spark |
| `gold.py` | silver → gold: daily, lịch chuẩn + forward-fill, tương quan, yield curve, ML features |
| `cassandra_sink.py` | Chọn và ép kiểu cột theo `contracts/cassandra.cql`, upsert vào Cassandra |
| `ml.py` | KMeans (StandardScaler + KMeans + silhouette) |
| `jobs/*.py` | Entry point của từng job (spark-submit / SparkApplication) |
| `jobs/benchmark.py` | Đo trước/sau: pruning, shuffle partitions, AQE, broadcast, cache |
| `dags/market_lakehouse_dag.py` | DAG Airflow chạy lúc 01:00 UTC mỗi ngày |
| `k8s/*.yaml` | Template SparkApplication cho từng job (Spark Operator) |
| `Dockerfile` | Image `btl/batch:dev` (jar s3a + Cassandra connector có sẵn) |
| `transforms.py`, `run_local_batch.py` | Bản pandas để chạy trên máy không có Java |

## Đường dẫn và phân vùng

- `dt` luôn là **ngày UTC**. Ở bronze/silver đó là ngày của sự kiện; ở gold đó là ngày tính.
  Session Spark được ghim `spark.sql.session.timeZone=UTC`.
- Bronze được đọc **theo từng topic** (`bronze/<topic>/dt=<ngày>/`). Đọc thẳng gốc
  `bronze/` thì Spark báo `Conflicting directory structures detected`; lỗi này có test
  tái hiện.
- Ghi bằng `partitionOverwriteMode=dynamic`: chạy lại một ngày chỉ thay đúng phân vùng
  của ngày đó (idempotent).
- Riêng `silver/alerts`: `dt` là phân vùng bronze lúc cảnh báo đến (sink alerts phân vùng
  theo thời điểm ghi vào Kafka), còn cột `day` là ngày của cảnh báo.

## Luật làm sạch (bronze → silver)

Mỗi dòng nhận một `reject_reason`; NULL nghĩa là hợp lệ. Dòng bị loại không vào silver,
và số lượng theo từng lý do được ghi vào `silver/_stats/dt=…` (dùng cho số liệu "% bản ghi
bị chặn").

| Bảng | Luật | Khóa bỏ trùng (bản được giữ) |
|---|---|---|
| trades | JSON hỏng, thiếu `trade_id`, symbol sai mẫu, asset_class ngoài hợp đồng, price ≤ 0, volume < 0, ngày lệch phân vùng | `trade_id` (bản ingest sớm nhất) |
| bars | như trên, cộng timeframe ∉ {1Min, 1Day} và `high < max(open, close, low)` hoặc `low > min(open, close)` | `bar_id` |
| yields | series_id/tenor không khớp, `day` sai định dạng, yield NULL | `(series_id, day)` (bản ingest **mới nhất**, vì FRED có sửa số) |
| alerts | thiếu id, symbol, thời gian; alert_type/severity ngoài hợp đồng | `alert_id` |

Dòng có ngày sự kiện khác phân vùng bronze bị loại (`dt_mismatch`). Nếu không loại, ghi
dynamic overwrite sẽ thay một ngày không được yêu cầu bằng một bản chỉ có một phần dữ liệu.

## Gold: thứ tự ưu tiên nguồn, lịch chuẩn, forward-fill

**Giá ngày** (`daily_summary`): mỗi `(symbol, day)` lấy đúng một nguồn, theo thứ tự ưu tiên:

1. `bar_1d`: nến ngày yfinance (backfill). Khối lượng lấy từ băng tổng hợp toàn thị trường
   Mỹ, trong khi feed IEX miễn phí chỉ thấy vài phần trăm.
2. `trades`: tổng hợp từ giao dịch realtime, cho những ngày backfill chưa phủ.
3. `bar_1m`: nến phút Alpaca, chỉ dùng khi luồng trade bị đứt mà luồng nến vẫn chạy.

Cột `price_source` trong gold cho biết dòng đến từ nguồn nào. VWAP = Σ(giá·KL)/ΣKL, và để
NULL khi khối lượng bằng 0 (DXY).

**Tương quan** (`correlation_matrix`), trả lời các câu hỏi trong slide "So sánh với trái
phiếu chính phủ Mỹ":

- Lịch chuẩn là các ngày SPY giao dịch. Ngày cuối tuần của crypto bị loại khi join, nên
  mọi chuỗi được đo trên cùng các ngày giao dịch ở Mỹ.
- Lợi suất là các "mã giả" `US2Y`, `US10Y`, `REAL10Y`. Ngày thị trường trái phiếu nghỉ mà
  sàn cổ phiếu vẫn mở (Columbus Day, Veterans Day) được **forward-fill tối đa 3 ngày**
  (`last(..., ignorenulls)` trên window 3 dòng), để một mức cũ không bị kéo qua cả một đợt
  mất dữ liệu dài.
- Giá tính trên lợi suất ngày, còn lợi suất tính trên **thay đổi** (điểm %).
- Cửa sổ là 90 dòng lịch gần nhất tính đến ngày xử lý. Mỗi cặp xuất hiện một lần
  (`base_symbol < quote_symbol`), kèm `n_obs`. `as_of` là ngày lịch cuối cùng của cửa sổ
  (chạy vào thứ Bảy thì `as_of` là thứ Sáu).

**Đường cong lợi suất** (`yield_curve`): pivot 9 kỳ hạn thành `y_1m … y_30y`, `real_10y`,
cùng `spread_10y_2y`, `spread_10y_3m` và `inverted` (= spread 10Y−2Y âm).

**Lợi suất công bố trễ**: FRED/Treasury công bố lợi suất của ngày D vào ngày làm việc sau
đó. Vì vậy mỗi lần chạy đều đọc lại 7 ngày gần nhất của `rates.yields`
(`YIELD_LATENCY_DAYS`), ghi lại 7 phân vùng `yield_curve` cuối, và sink cũng chép lại
đúng 7 ngày đó.

Mọi phép tính ở gold đều có bản tham chiếu pandas trong `common/analytics.py`.
`tests/test_gold.py` yêu cầu hai bên khớp nhau trong sai số 1e-6.

## Cổng chất lượng

`jobs/quality_checks.py` chạy giữa silver và gold, ghi báo cáo JSON vào
`gold/_quality/dt=<ngày>/`, và thoát với mã 1 khi có lỗi:

| Luật | Mức |
|---|---|
| Khóa NULL, price ≤ 0, high < low, khóa trùng còn lại trong silver | error |
| `crypto.trades` rỗng (crypto chạy 24/7) | error |
| Topic cổ phiếu/vàng/DXY rỗng vào ngày thị trường mở | error |
| Topic đó rỗng vào cuối tuần hoặc ngày lễ NYSE | warning |
| `stocks.bars` rỗng (khi không có key Alpaca) | warning |
| `rates.yields` của ngày D rỗng | warning (lợi suất công bố trễ) |
| Không có lợi suất nào trong 8 ngày gần nhất | error |
| Tỉ lệ dòng bronze bị loại > 5% / > 20% | warning / error |
| Dòng crypto mới nhất cách cuối ngày hơn 2 giờ | warning |

Danh sách ngày lễ NYSE 2026 nằm trong `quality.py` (có comment `NOTICE`) và phải cập nhật
mỗi năm. Có thể thay cả bộ luật bằng Great Expectations (PDF ý tưởng); định dạng báo cáo
được giữ đơn giản để làm việc đó.

## MLlib: phân cụm tài sản

`ml.py` dùng 3 feature cho mỗi mã: lợi suất trung bình, độ biến động (90 ngày giao dịch
của chính mã đó) và tương quan với thay đổi lợi suất 10 năm. Feature được chuẩn hoá bằng
StandardScaler rồi đưa vào KMeans (k=3, seed 42). Kết quả kèm silhouette. Mã thiếu feature
(ví dụ tương quan có dưới 3 ngày chung) bị loại khỏi mô hình của ngày đó, không bị điền
giá trị giả. Trên thị trường giả lập, TLT/IEF chung một cụm, BTC/ETH chung một cụm khác.

## Chạy

### Laptop không có Java (pandas)

```bash
python batch/run_local_batch.py     # ghi data/batch/silver, data/batch/gold
```

### Spark local trên file (cần Java 17)

```bash
export LAKE=file:///tmp/lake        # bronze/ do Kafka Connect hoặc script sinh dữ liệu tạo
python batch/jobs/bronze_to_silver.py --lake $LAKE --start 2026-09-01 --end 2026-11-29 --master 'local[2]'
python batch/jobs/quality_checks.py   --lake $LAKE --dt 2026-11-25 --master 'local[2]'
python batch/jobs/silver_to_gold.py   --lake $LAKE --dt 2026-11-25 --master 'local[2]'
python batch/jobs/cassandra_sink.py   --lake $LAKE --dt 2026-11-25 --dry-run --master 'local[2]'
python batch/jobs/train_kmeans.py     --lake $LAKE --dt 2026-11-25 --dry-run --master 'local[2]'
```

### Với MinIO / Cassandra của Docker Compose

```bash
export MINIO_ENDPOINT=http://localhost:9000 MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin
export CASSANDRA_HOSTS=127.0.0.1
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  batch/jobs/silver_to_gold.py --lake s3a://lake --dt 2026-11-25
```

Thông tin đăng nhập MinIO **chỉ** lấy từ biến môi trường, không truyền qua tham số dòng
lệnh, vì tham số sẽ lộ trong `ps` và trong spec của SparkApplication.

### Trên Kubernetes

```bash
docker buildx build --platform linux/arm64 -f batch/Dockerfile -t btl/batch:dev .
minikube image load btl/batch:dev
```

Airflow nộp các template trong `batch/k8s/` (namespace `spark`, serviceAccount
`spark-operator-spark`). Xem `batch/airflow/README.md`.

## Tối ưu hiệu năng

`jobs/benchmark.py` đo trước/sau 5 kỹ thuật; kết quả nằm ở `docs/metrics/C-batch.md`. Đo
local: partition pruning ×2,4, giảm shuffle partition từ 200 xuống 8 ×5,6, AQE ×2,1 (khi
giữ 200 partition), cache ×1,8, broadcast ×1,04 (SortMergeJoin 7 → 3). Cấu hình chạy thật
giữ AQE bật, `spark.sql.shuffle.partitions=8`, lọc theo cột `dt` và cache các DataFrame
được dùng lại.

## Test

```bash
python -m pytest batch/tests -q
```

- Test Spark tự skip khi không có pyspark hoặc Java.
- `test_ml.py` cần `pyspark.ml`. Trên Python 3.12, module này cần setuptools (shim
  `distutils`); image `btl/batch:dev` dùng Python 3.10 nên không bị ảnh hưởng.

## Giới hạn hiện tại

- Chưa chạy trên cụm thật. Các SparkApplication đã qua `kubeconform -strict` với CRD,
  nhưng env lấy từ secret cần webhook của Spark Operator đang bật.
- Danh sách ngày lễ chỉ có năm 2026.
- Số liệu benchmark là số đo local; phải đo lại trên cụm.
