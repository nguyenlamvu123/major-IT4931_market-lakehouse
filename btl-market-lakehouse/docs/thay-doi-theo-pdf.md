# Thay đổi và bổ sung theo PDF (hợp đồng v2)

Cập nhật: 12/11/2026. File này ghi lại toàn bộ những gì đã sửa và bổ sung để repo bám sát
hai tài liệu của môn:

- **PDF Kiến trúc**: `BTL-IT4931_Kien-truc-Phan-cong-GitHub.pdf`, phiên bản 09/11/2026 (tài
  liệu nhập môn: hợp đồng giữa các vai, phân công A/B/C/D, kế hoạch 6 tuần).
- **PDF Ý tưởng**: `Ý tưởng BTL Market Lakehouse.pdf` (18 slide đề xuất).

Nếu hai PDF nói khác nhau, lấy **PDF Kiến trúc** làm chuẩn vì nó mới hơn và cụ thể hơn:

- Lưu Parquet thường trên MinIO, không dùng Iceberg + Trino.
- Watermark 30 giây, không phải 2 phút.
- Tên topic và tên bảng theo mục "Hợp đồng giữa các vai".

Một số ý của PDF Ý tưởng vẫn được giữ:

- `dead-letter`;
- TTL Cassandra 7 ngày;
- forward-fill lịch nghỉ thị trường;
- kiểm tra chất lượng dữ liệu giữa silver và gold;
- 3 broker với RF 3 cho bản demo.

Quy ước tên nhánh của PDF (`<thư-mục>/<issue>-<mô-tả>`, PR vào `main`) **tạm gác lại**
theo yêu cầu. Vì vậy vẫn giữ mô hình cũ: nhánh lớn theo vai, nhánh con merge `--no-ff` vào
nhánh lớn, nhánh vai không merge vào `main`. Chưa push nhánh nào.

## 1. Kiểm thử: trước và sau

Môi trường kiểm thử là môi trường **tạm**, đặt trong thư mục scratch và **đã xoá** sau khi
chạy xong. Gồm:

- venv Python 3.12 (pandas 3.1, pyspark 3.5.3, pytest, jsonschema, fastapi, httpx…);
- JDK 17 Temurin, để Spark chạy local thật;
- `kubeconform` 0.6.7 và `kustomize` 5.4.3.

Máy không có docker, kubectl hay helm, nên chưa có gì chạy trên cụm hoặc container thật.

| Nhánh | Trước khi sửa | Sau khi sửa | ruff | kubeconform |
|---|---|---|---|---|
| `main` | 5 pass | 26 pass | sạch | – |
| `feature/A-ingest` | 10 pass | 73 pass | sạch | 14/14 hợp lệ |
| `feature/B-streaming` | 7 pass | 60 pass (gồm test Spark thật) | sạch | 3/3 hợp lệ |
| `feature/C-batch` | 9 pass | 59 pass (gồm test Spark thật) | sạch | 5/5 template hợp lệ |
| `feature/D-infra` | 7 pass | 127 pass | sạch | 35/36 hợp lệ, 1 bỏ qua (`K8ssandraCluster` chưa có schema công khai) |

Bộ test cũ pass hết nhưng **không có test nào chạm tới Spark**. Vì vậy chúng không phát hiện
được các lỗi chặn ở mục 4. Mỗi lỗi đó giờ có một test tái hiện: fail trước khi sửa, pass sau
khi sửa.

## 2. Hợp đồng v2

Hợp đồng nằm trong `contracts/` **trên `main`**, nên cả 4 nhánh vai đọc chung một bản.
`common/contracts.py` là bản thực thi bằng Python; `tests/test_contracts.py` kiểm tra YAML,
JSON Schema, dữ liệu mẫu và CQL luôn khớp nhau.

**Kafka.** Cụm Strimzi tên `market`, địa chỉ `market-kafka-bootstrap.kafka.svc:9092`.

| Topic | Key | Nguồn | Mã |
|---|---|---|---|
| `crypto.trades` | symbol | Binance WS | BTCUSDT, ETHUSDT |
| `stocks.trades` | symbol | Alpaca IEX WS (Finnhub WS dự phòng) | AAPL, MSFT, NVDA, SPY, GLD, TLT, IEF, SHY |
| `stocks.bars` | symbol | Alpaca nến 1Min; backfill yfinance nến 1Day cho mọi mã | như trên + backfill |
| `gold.quotes` | symbol | Yahoo | GC=F |
| `fx.dxy` | symbol | Yahoo `DX-Y.NYB` | DXY |
| `rates.yields` | tenor | FRED (có key) hoặc CSV Treasury (không cần key) | 1M…30Y, REAL10Y |
| `alerts` | symbol | Speed layer (B) | |
| `dead-letter` | source | Producer, Kafka Connect | bản ghi sai hợp đồng |

Topic chọn theo **nguồn feed**, còn `asset_class` (crypto, equity, gold, bond, fx, rates) mô
tả **tài sản gốc**. Ví dụ GLD đi qua `stocks.trades` nhưng có `asset_class=gold`.

Khóa tất định dùng để bỏ trùng:

- `trade_id`: tính từ id giao dịch của sàn nếu có.
- `bar_id` và `alert_id`: tính từ nội dung bản ghi.

**MinIO.**

- Bronze: `s3a://lake/bronze/<topic>/dt=YYYY-MM-DD/`.
- Silver: `s3a://lake/silver/{trades,bars,yields,alerts}/dt=…/`.
- Gold: `s3a://lake/gold/{daily_summary,correlation_matrix,yield_curve,ml_features,asset_clusters}/dt=…/`.
- Checkpoint streaming: `s3a://checkpoints/<job>/`.

**Cassandra** (keyspace `market`, file `contracts/cassandra.cql`):

- Realtime, B ghi, TTL 7 ngày: `ohlcv_1m`, `ohlcv_5m`, `indicators_latest`, `alerts_by_day`.
  Bảng nến dùng partition `(symbol, day)` để trả lời đúng câu truy vấn "giá BTC hôm nay theo
  từng phút".
- Batch, C ghi: `daily_summary`, `correlation_matrix`, `yield_curve`, `asset_clusters`.

**Hạ tầng** (`contracts/infra.yaml`):

- Namespace: `kafka`, `minio`, `cassandra`, `spark`, `airflow`, `monitoring`, `market`.
- Secret: `market-api-keys`, `minio-credentials`.
- 6 image tag `:dev`, build cho linux/arm64.
- Cổng metrics: producer 8001, driver streaming 9108, market-api 8000.
- Lịch batch `0 1 * * *` (01:00 UTC = 08:00 giờ VN, sau khi phiên Mỹ đóng lúc 04:00).

## 3. Danh sách nhánh con

Mỗi nhánh con được tạo từ nhánh đích, sau đó merge `--no-ff` trở lại.

| Nhánh con | Nhánh đích | Nội dung |
|---|---|---|
| `feature/common-contract-v2` | `main` | `common/` theo hợp đồng v2. Gồm: registry mã; `trade_id` theo id sàn; validator cho bar/rate; z-score so với 30 nến trước; bỏ trùng trong `aggregate_trades`; lịch SPY + forward-fill; tương quan với lợi suất; đường cong lợi suất đủ kỳ hạn; thị trường giả lập; 20 mẫu/topic |
| `feature/common-contracts-dir` | `main` | Chuyển `contracts/` lên `main` (topics, 4 schema, CQL, paths, infra, samples), thêm `common/cql.py`, `tests/test_contracts.py` |
| `feature/common-bars-backfill` | `main` | `stocks.bars` chứa thêm nến ngày backfill của mọi mã |
| `feature/common-infra-feedback-a` | `main` | Thêm metric `market_producer_duplicates_total`, 2 consumer group của S3 sink, DFII10 lấy từ CSV Treasury |
| `feature/common-requirements-setuptools` | `main` | Thêm `setuptools`, vì pyspark 3.5 vẫn `import distutils` mà Python 3.12 đã bỏ |
| `feature/docs-pdf-alignment` | `main` | File này và README |
| `feature/A-contracts-v2` | `feature/A-ingest` | Xoá các file hợp đồng v1 |
| `feature/A-producers-v2` | `feature/A-ingest` | Producer theo v2: Alpaca và Finnhub chuyển sang WebSocket; Yahoo cho GC=F/DXY; FRED đủ 9 series; thêm producer CSV Treasury; luôn gắn `ingest_time` |
| `feature/A-resilience` | `feature/A-ingest` | Backoff + jitter, circuit breaker, `dead-letter`, LRU chống trùng, metrics trên cổng 8001 |
| `feature/A-replay-speed` | `feature/A-ingest` | Replay 1x–100x, `--rebase-time`, `--loop`, đọc JSONL hoặc bronze MinIO |
| `feature/A-backfill` | `feature/A-ingest` | `backfill_yfinance.py`: nến 1Day nhiều năm cho mọi mã |
| `feature/A-kafka-connect-v2` | `feature/A-ingest` | Sửa S3 sink; tách connector riêng cho `alerts`; image `btl/kafka-connect-s3`; CR `KafkaConnect`/`KafkaConnector` của Strimzi |
| `feature/A-k8s-deploy` | `feature/A-ingest` | Image `btl/ingest:dev`; Deployment cho từng nguồn; Job backfill; ServiceMonitor |
| `feature/A-docs` | `feature/A-ingest` | README; công cụ đo `topic_rates.py`, `feed_quality.py`, `bench_publish.py`; khung chương báo cáo |
| `feature/A-replay-speed-env` | `feature/A-ingest` | Tích hợp với D: tốc độ replay đọc từ biến `REPLAY_SPEED` |
| `feature/B-contract-v2` | `feature/B-streaming` | Schema v2, watermark 30 s, `dropDuplicatesWithinWatermark(trade_id)`, `--source files`, ghi đúng cột của từng bảng |
| `feature/B-ohlcv-update-mode` | `feature/B-streaming` | Nến 1m và 5m, output mode update, trigger 5 s, checkpoint riêng cho mỗi query |
| `feature/B-stateful-indicators` | `feature/B-streaming` | `IndicatorState` giữ 60 nến/mã qua các batch, nạp lại từ Cassandra khi khởi động lại; ghi `indicators_latest`, `alerts_by_day` và Kafka `alerts` |
| `feature/B-metrics-latency` | `feature/B-streaming` | 5 gauge trên cổng 9108; `measure_latency.py`, `exactly_once_check.py` |
| `feature/B-k8s-sparkapp` | `feature/B-streaming` | Image `btl/streaming:dev` đóng sẵn jar; SparkApplication `speed-layer`; Service; PodMonitor |
| `feature/B-docs` | `feature/B-streaming` | README, khung chương báo cáo, lessons, số liệu |
| `feature/C-bronze-silver-v2` | `feature/C-batch` | Đọc bronze theo từng topic và từng `dt`; silver trades/bars/yields/alerts; ghi đè theo phân vùng `dt` |
| `feature/C-gold-fix` | `feature/C-batch` | Viết lại gold bằng Spark (daily, tương quan có lợi suất, yield curve), kết quả khớp bản tham chiếu pandas |
| `feature/C-cassandra-sink` | `feature/C-batch` | Ghi 4 bảng batch; cột lấy từ `contracts/cassandra.cql` |
| `feature/C-quality-checks` | `feature/C-batch` | Cổng chất lượng giữa silver và gold: báo cáo `gold/_quality`, exit 1 khi có lỗi |
| `feature/C-mllib-v2` | `feature/C-batch` | KMeans (mean_return, volatility, corr_us10y), tính silhouette |
| `feature/C-perf-benchmark` | `feature/C-batch` | `benchmark.py`: số đo trước/sau cho AQE, broadcast, partition, cache, pruning |
| `feature/C-airflow-v2` | `feature/C-batch` | DAG `0 1 * * *` dùng `SparkKubernetesOperator`, 5 template `batch/k8s`, image `btl/batch:dev` |
| `feature/C-docs` | `feature/C-batch` | README batch/airflow, khung chương báo cáo, 6 lessons |
| `feature/D-compose-v2` | `feature/D-infra` | Bỏ `bitnami/*`, dùng `apache/kafka` có 2 listener; topic sinh từ `topics.yaml`; init Cassandra và MinIO bằng service trong compose |
| `feature/D-k8s-operators` | `feature/D-infra` | Strimzi, K8ssandra, Spark Operator, Airflow (KubernetesExecutor), kube-prometheus-stack, MinIO; KafkaTopic sinh tự động; RBAC |
| `feature/D-make-up` | `feature/D-infra` | `make up/down/images/secrets/status/port-forward` |
| `feature/D-serving-v2` | `feature/D-infra` | market-api đọc đủ 8 bảng; chỉ fallback sang demo khi `DEMO_FALLBACK=true`; header `X-Data-Source` |
| `feature/D-dashboards-v2` | `feature/D-infra` | Dashboard thị trường (nến, VWAP, chỉ báo, cảnh báo, tương quan, lợi suất) và dashboard giám sát (Kafka lag, Spark, độ trễ, CPU/RAM) |
| `feature/D-github` | `feature/D-infra` | `.github/` đặt ở gốc git: CI, CODEOWNERS, mẫu PR |
| `feature/D-fault-injection-k8s` | `feature/D-infra` | 6 kịch bản thử lỗi trên K8s, kèm bảng ghi kết quả |
| `feature/D-contract-sync` | `feature/D-infra` | Strimzi 0.45 / Kafka 3.9; `minio-credentials` trong ns kafka; chờ topic Ready; panel lag và panel duplicates |
| `feature/D-docs-v2` | `feature/D-infra` | architecture, runbook, metrics, report_outline, demo_script, lessons |
| `feature/D-drop-temp-metric-allowance` | `feature/D-infra` | Bỏ ngoại lệ tạm trong test dashboard sau khi đã merge `main` |
| `feature/D-airflow-batch-templates` | `feature/D-infra` | Tích hợp với C: image Airflow đặt `MARKET_BATCH_K8S_DIR` |
| `feature/D-metrics-crossrefs` | `feature/D-infra` | `docs/metrics.md` trỏ đúng file số liệu của từng vai và ghi số đo local |

## 4. Lỗi chặn đã sửa

| # | Lỗi | Ở đâu | Cách sửa | Test tái hiện |
|---|---|---|---|---|
| 1 | Job silver→gold group theo `window_start`/`open`/`close` trong khi silver là bảng giao dịch, gây `UNRESOLVED_COLUMN` | C | Tính daily từ trades bằng Spark | Có |
| 2 | Đọc thẳng gốc `s3a://lake/bronze` có nhiều thư mục topic gây "Conflicting directory structures" | C | Đọc theo `<topic>/dt=` | Có |
| 3 | Ghi 21 cột vào bảng `ohlcv_1m` chỉ có 15 cột, connector Cassandra từ chối | B | Mỗi bảng ghi đúng cột hợp đồng, sink kiểm tra cột lúc chạy | Có |
| 4 | Chỉ báo tính trong từng micro-batch, mỗi batch chỉ có khoảng 1 nến, nên z-score luôn NaN và không bao giờ phát cảnh báo | B | Giữ trạng thái 60 nến/mã qua các batch | Có |
| 5 | Append + watermark 2 phút làm nến vào Cassandra chậm ≥ 2–3 phút (PDF yêu cầu < ~10 s) | B | Output mode update, trigger 5 s; đo local p95 2,1 s | Có (test Spark) |
| 6 | `ci.yml` nằm trong `btl-market-lakehouse/.github`, GitHub không đọc nên CI không bao giờ chạy | D | Chuyển `.github/` ra gốc git, đặt `working-directory` | – |
| 7 | Alpaca REST không có `start`, `feed=iex`, phân trang, nên lặp lại 100 trade đầu ngày | A | Chuyển sang WebSocket; REST có con trỏ thời gian và phân trang | Có |
| 8 | S3 sink thiếu `locale`/`timezone`/`partition.duration.ms`; `Record` bỏ qua `event_time` | A | `RecordField` trên `event_time`, đủ khóa bắt buộc | Có |
| 9 | `register_connector.sh` PUT cả `{name, config}` vào `/config` | A | Chỉ gửi phần `config` | – |
| 10 | `kustomization.yaml` cũ đọc file nằm ngoài thư mục gốc nên không build được; `init-cassandra.sh` gọi sai tên service và sai đường dẫn CQL | D | Viết lại | Có (`kustomize build`) |
| 11 | Image `bitnami/*` có thể không pull được nữa (Bitnami rút catalog miễn phí năm 2025) | D | Dùng `apache/kafka`, `apache/spark` | – |
| 12 | market-api âm thầm trả dữ liệu giả khi mất Cassandra | D | Chỉ fallback khi `DEMO_FALLBACK=true` (K8s đặt false); thêm header `X-Data-Source` | Có |
| 13 | Hai giao dịch thật trùng thời gian/giá/khối lượng bị gộp làm một; DXY không có volume nên VWAP chia cho 0 | `common` | `trade_id` theo id sàn; VWAP để null khi volume = 0 | Có |
| 14 | Lợi suất chỉ có 4 kỳ hạn, thiếu lợi suất thực; tương quan không có lợi suất, không căn lịch nghỉ | `common`, C | 9 series; lịch SPY; forward-fill tối đa 3 ngày | Có |
| 15 | Image Airflow chép DAG vào `/opt/airflow/dags` nhưng DAG tìm template ở `dags/../k8s` | D↔C | `MARKET_BATCH_K8S_DIR=/opt/app/batch/k8s` | – |
| 16 | Script dồn tải đổi `REPLAY_SPEED`, nhưng Deployment replay ghi cứng `--speed 10` | A↔D | Tham số đọc `$(REPLAY_SPEED)` | Có |
| 17 | pyspark 3.5 gọi `toPandas()` lỗi trên Python 3.12 vì thiếu `distutils` | `main` | Thêm `setuptools` vào requirements | – |

## 5. Đối chiếu việc trong PDF Kiến trúc

Ký hiệu trạng thái:

- ✅ đã có code và test;
- 🔧 đã có công cụ hoặc manifest, nhưng **cần cụm thật** mới lấy được số hoặc kết quả;
- 👤 việc của con người.

| Việc (mục 5 của PDF) | Trạng thái | Ở đâu |
|---|---|---|
| A.1 Schema + 20 dòng mẫu mỗi topic | ✅ | `contracts/` trên `main` |
| A.2 Producer gửi Kafka; Alpaca/Finnhub cho cổ phiếu và GLD, TLT, IEF, SHY | ✅ (chưa gọi API thật) | `ingest/producers/` |
| A.3 Producer replay tốc độ tuỳ chỉnh | ✅ | `ingest/producers/replay.py` |
| A.4 Kafka Connect S3 sink sang bronze | ✅ config / 🔧 chạy thật | `ingest/kafka_connect/`, `ingest/k8s/kafka-connect/` |
| A.5 Dockerfile + Deployment K8s | ✅ manifest / 🔧 build image | `ingest/Dockerfile`, `ingest/k8s/` |
| A.6 Đo tin nhắn/giây, xử lý mất kết nối và trùng tin | ✅ code / 🔧 số trên cụm | `ingest/tools/`, `docs/metrics/A-ingest.md` |
| B.1 Spark local đọc file mẫu ra nến 1 phút | ✅ | `streaming/job.py --source files` |
| B.2 Đọc Kafka, watermark, nến 1m và 5m | ✅ / 🔧 Kafka thật | `streaming/` |
| B.3 Bảng Cassandra realtime | ✅ | `contracts/cassandra.cql`, `streaming/sinks.py` |
| B.4 VWAP, lợi suất, độ biến động trượt, cảnh báo | ✅ | `streaming/state.py` |
| B.5 Chứng minh exactly-once (xoá pod) | ✅ local (checkpoint) / 🔧 trên cụm | `streaming/tools/exactly_once_check.py`, kịch bản thử lỗi #2 |
| B.6 Đo độ trễ | ✅ local p95 2,1 s / 🔧 trên cụm | `streaming/tools/measure_latency.py` |
| C.1 Cấu trúc bronze/silver/gold, đọc file mẫu | ✅ | `contracts/paths.yaml`, `batch/` |
| C.2 bronze→silver: làm sạch, bỏ trùng, UTC, phân vùng ngày | ✅ | `batch/jobs/bronze_to_silver.py` |
| C.3 silver→gold: giá ngày, tương quan với lợi suất, đường cong lợi suất | ✅ | `batch/gold.py` |
| C.4 Ghi bảng batch sang Cassandra | ✅ / 🔧 Cassandra thật | `batch/jobs/cassandra_sink.py` |
| C.5 Mô hình MLlib | ✅ KMeans | `batch/jobs/train_kmeans.py` |
| C.6 Tối ưu hiệu năng có số liệu trước/sau | ✅ số local / 🔧 số trên cụm | `docs/metrics/C-batch.md` |
| C.7 DAG Airflow chạy mỗi đêm | ✅ DAG / 🔧 chạy 3 đêm liên tiếp | `batch/dags/` |
| D.1 Repo, mời thành viên, bảo vệ nhánh `main` | 👤 làm trên GitHub Settings; đã có CODEOWNERS và mẫu PR | `.github/` |
| D.2 Dựng cụm: Kafka, MinIO, Cassandra, Spark Operator, Airflow, Prometheus/Grafana | ✅ manifest / 🔧 `make up` | `infra/k8s/` |
| D.3 `make up` / `make down` | ✅ / 🔧 chạy trên máy sạch | `Makefile`, `infra/scripts/` |
| D.4 Docker Compose nhẹ cho laptop | ✅ / 🔧 `docker compose up` | `infra/docker-compose.dev.yml` |
| D.5 Dashboard thị trường + dashboard giám sát | ✅ JSON / 🔧 xem trên Grafana | `infra/monitoring/` |
| D.6 Thử lỗi | ✅ kịch bản / 🔧 chạy và ghi kết quả | `infra/fault_injection/k8s/` |
| D.7 Ghép báo cáo, slide, video demo | 👤 đã có khung | `docs/report_outline.md`, `docs/report/`, `docs/demo_script.md` |
| Việc chung: chương báo cáo 4 phần, `docs/metrics.md`, `docs/lessons/` | Đã có khung / 👤 điền số | `docs/` |

Đã bổ sung thêm theo PDF Ý tưởng:

- `dead-letter`;
- backfill nhiều năm;
- lợi suất thực DFII10;
- lịch nghỉ khác nhau giữa các thị trường;
- kiểm tra chất lượng dữ liệu giữa silver và gold;
- TTL 7 ngày;
- circuit breaker;
- 3 câu hỏi "so sánh với trái phiếu", trả lời được trên dữ liệu giả lập. Ví dụ: TLT so với
  US10Y là −0,83; GLD so với lợi suất thực là −0,70.

**Chưa làm** (PDF Kiến trúc không yêu cầu): Iceberg/Trino, SEC EDGAR, tin tức,
GraphFrames/PageRank, Great Expectations (đã thay bằng cổng chất lượng tự viết), Kafka TLS/ACL.

## 6. Số liệu đã đo (local, chưa phải trên cụm)

| Vai | Đo | Kết quả |
|---|---|---|
| A | Đường publish (validate + định tuyến + chống trùng), không qua Kafka | ≈ 204 nghìn bản ghi/s |
| B | Độ trễ giao dịch → nến 1m (Spark local[2], nguồn file, trigger 5 s) | p50 0,48 s; p95 2,10 s; xấu nhất ước tính ≈ 9,3 s; ~940 dòng/s |
| C | Tối ưu batch (Spark local[2], dữ liệu giả lập) | pruning ×2,44; 200→8 partition ×5,60; AQE ×2,09; broadcast ×1,04; cache ×1,83 |
| D | market-api ở chế độ demo, gọi qua TestClient | p95 6–20 ms mỗi endpoint |

## 7. Việc còn lại

**Cần cụm thật** (minikube 16 GB trên MacBook M1 Max):

1. `make images`, `make secrets`, `make up`. Lần đầu sẽ phải thử các phiên bản ghi theo
   tài liệu chứ chưa chạy:
   - chart k8ssandra-operator 1.20.2, kube-prometheus-stack 61.9.0, Strimzi 0.45.0 / Kafka 3.9.0;
   - tag `minio/minio`, `danielqsj/kafka-exporter` v1.7.0;
   - base `apache/spark:3.5.3-…`;
   - URL plugin S3 trên Confluent Hub và URL CSV của Treasury.
2. Chạy 4 nguồn liên tục 1 giờ, lấy số tin nhắn/giây mỗi topic (A.6).
3. Đo độ trễ và exactly-once với Kafka + Cassandra (B.5, B.6).
4. DAG chạy 3 đêm liên tiếp; đo lại benchmark trên cụm (C.6, C.7).
5. Chạy 6 kịch bản thử lỗi và ghi vào `docs/lessons/` (D.6).

**Việc của con người:**

- Điền username thật vào `.github/CODEOWNERS` (đang là placeholder `@ban_A..D`).
- Bật bảo vệ nhánh trên GitHub.
- Tạo API key Alpaca, Finnhub, FRED và đặt vào `.env`.
- Viết báo cáo, slide, quay video demo.
- Quyết định quy ước nhánh và cách tích hợp: tạo nhánh `integration`, hoặc PR vào `main` theo PDF.

## 8. Rủi ro đã biết

- pyspark 3.5 chạy với pandas 3.x vẫn pass test nhưng sinh khoảng 1.300 cảnh báo deprecation
  từ bên trong pyspark. Nếu gặp lỗi lạ khi chuyển đổi DataFrame, ghim `pandas<3` trong image.
- PodMonitor `market-workloads` của D chọn nhãn `btl.market/metrics`, nhưng A và B dùng
  ServiceMonitor/PodMonitor riêng. Hiện không có pod nào bị scrape hai lần; nếu thêm nhãn
  này vào pod của A hoặc B thì phải bỏ monitor riêng của vai đó.
- Các nhánh vai chưa được tích hợp với nhau. Quy ước chéo (tên Deployment, cổng, secret,
  Dockerfile, đường dẫn template) đã được kiểm tra thủ công bằng cách đọc chéo các nhánh,
  chưa có test tự động. Khi tạo nhánh `integration`, nên chạy lại toàn bộ test trên đó.

## 9. Chạy lại kiểm thử

```bash
cd btl-market-lakehouse
python3 -m venv .venv && . .venv/bin/activate      # Ubuntu thiếu ensurepip: python3 -m venv --without-pip rồi get-pip.py
pip install -r requirements.txt
python scripts/generate_samples.py && python -m pytest -q
python scripts/run_local_demo.py && python scripts/verify_pipeline.py
# Test Spark (nhánh B, C) cần Java 17: export JAVA_HOME=...
```
