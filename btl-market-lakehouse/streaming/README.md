# Speed layer (vai B)

**Một câu:** biến dòng giao dịch rời rạc thành nến giá, chỉ báo và cảnh báo trong vài giây
(PDF *Kiến trúc – Phân công*, thẻ vai B).

- **Dùng khi:** cần dữ liệu "nóng" cho dashboard realtime (nến 1 phút/5 phút, chỉ báo mới
  nhất, cảnh báo hôm nay).
- **Không dùng khi:** cần số liệu chính xác theo ngày, tương quan nhiều tháng hay ML. Phần đó
  thuộc batch layer (vai C), chạy lại toàn bộ dữ liệu trong MinIO mỗi đêm.

## Luồng dữ liệu

```text
Kafka crypto.trades, stocks.trades, gold.quotes, fx.dxy  (hoặc file contracts/samples/*.jsonl)
  │  from_json(trade schema v2) → bỏ bản ghi sai hợp đồng → event_ts (UTC)
  │  watermark 30 s + dropDuplicatesWithinWatermark(trade_id)
  ├─► query speed-ohlcv-1m  (cửa sổ 1 phút, output mode update, trigger 5 s)
  │     foreachBatch: upsert ohlcv_1m
  │       → IndicatorState (60 nến gần nhất/mã, giữ qua các batch)
  │       → upsert indicators_latest, alerts_by_day; Kafka "alerts" (key = symbol)
  └─► query speed-ohlcv-5m  (cửa sổ 5 phút, output mode update, trigger 5 s)
        foreachBatch: upsert ohlcv_5m
```

Hai query đọc nguồn độc lập, mỗi query có checkpoint riêng: `s3a://checkpoints/speed-ohlcv-1m/`
và `s3a://checkpoints/speed-ohlcv-5m/`.

| File | Vai trò |
|---|---|
| `job.py` | Entrypoint: tham số CLI, SparkSession, khởi động 2 query, `SpeedLayer.process_bars` |
| `ohlcv.py` | Nguồn Kafka/file, làm sạch, watermark + bỏ trùng, gom nến OHLCV/VWAP |
| `state.py` | `IndicatorState`: bộ đệm nến theo mã, chỉ báo, cảnh báo, chống phát lặp lên Kafka |
| `sinks.py` | Cassandra (upsert), Parquet (chạy local), Kafka `alerts`, nạp lại state từ Cassandra |
| `columns.py` | Danh sách cột ghi vào từng bảng; đối chiếu với `contracts/cassandra.cql` |
| `metrics.py` | Gauge Prometheus `:9108` theo tên trong `contracts/infra.yaml` |
| `tools/` | Đo độ trễ, kiểm tra exactly-once, đo local |
| `k8s/speed-layer.yaml` | SparkApplication + Service + PodMonitor |

## Hợp đồng dùng (`contracts/`, nhánh `main`)

- Đọc: các topic dạng trade (`schemas/trade.schema.json`), key Kafka = `symbol`.
- Ghi Cassandra (keyspace `market`): `ohlcv_1m`, `ohlcv_5m`, `indicators_latest`, `alerts_by_day`.
  Mỗi lần ghi đều được kiểm tra cột so với hợp đồng: `streaming/columns.py::check_columns` chạy
  lúc runtime, còn `tests/test_contract_columns.py` chạy trong CI.
- Ghi Kafka: topic `alerts`, value theo `schemas/alert.schema.json` (đã có test bằng jsonschema).

## Chạy

### Tuần 1: Spark local, đọc file mẫu (việc B.1)

```bash
python scripts/generate_samples.py
spark-submit streaming/job.py --source files --input contracts/samples --available-now \
  --output-mode parquet --parquet-path /tmp/speed --checkpoint-root /tmp/checkpoints --metrics-port 0
```

Kết quả nằm trong `/tmp/speed/ohlcv_1m/`, `ohlcv_5m/`, `indicators_latest/`, `alerts_by_day/`
(Parquet). Mỗi lần upsert thêm một phiên bản; giá trị hiện tại của một nến là dòng có
`updated_at` mới nhất.

### Docker Compose (Kafka + Cassandra trên laptop)

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  streaming/job.py --bootstrap localhost:9092 --cassandra-host 127.0.0.1 \
  --checkpoint-root /tmp/checkpoints --starting-offsets earliest
```

Muốn để checkpoint trên MinIO thì đặt thêm `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`,
`MINIO_SECRET_KEY` và thêm package `org.apache.hadoop:hadoop-aws:3.3.4`.

### Kubernetes (Spark Operator)

```bash
docker buildx build --platform linux/arm64 -f streaming/Dockerfile -t btl/streaming:dev --load .
minikube image load btl/streaming:dev
kubectl apply -f streaming/k8s/speed-layer.yaml
kubectl -n spark get sparkapplication speed-layer
kubectl -n spark port-forward svc/speed-layer-metrics 9108:9108   # curl localhost:9108/metrics
```

Image đã có sẵn các jar Kafka, Cassandra connector, hadoop-aws nên khi khởi động không cần
tải gì từ Maven. Cần D chuẩn bị trước: Spark Operator (namespace `spark`, service account
`spark-operator-spark`), secret `minio-credentials` trong namespace `spark`, bucket
`checkpoints`, và bảng Cassandra đã tạo từ `contracts/cassandra.cql`. Nếu K8ssandra bật xác
thực, thêm `CASSANDRA_USERNAME`/`CASSANDRA_PASSWORD` vào `driver.env`.

Các tham số chính: `--watermark` (mặc định `30 seconds`), `--trigger` (`5 seconds`),
`--queries` (`speed-ohlcv-1m,speed-ohlcv-5m`), `--output-mode cassandra|parquet`,
`--metrics-port` (`9108`, đặt `0` để tắt).

## Quyết định thiết kế

### Watermark 30 giây

PDF quy định "thư đến trễ quá 30 giây thì không tính vào sổ phút trước". Giao dịch có
`event_time` cũ hơn (giờ sự kiện lớn nhất đã thấy − 30 s) không còn sửa được nến đã chốt,
và state của nến đó được giải phóng. Batch layer vẫn tính lại chính xác mỗi đêm từ bronze,
nên một giao dịch rất muộn chỉ lệch ở dashboard realtime chứ không mất khỏi hệ thống.

### Output mode `update` thay vì `append`

Tiêu chí hoàn thành của PDF: "nến 1 phút có trong Cassandra chậm dưới khoảng 10 giây so với
thời điểm giao dịch".

- `append` chỉ phát một cửa sổ sau khi watermark vượt qua thời điểm đóng cửa sổ. Nến
  10:00–10:01 vào Cassandra sớm nhất khi đã thấy giao dịch 10:01:30, tức trễ 30–90 s; bản v1
  dùng watermark 2 phút nên trễ 2–3 phút.
- `update` phát mọi nến thay đổi ở mỗi trigger (5 s), kể cả nến đang mở, và sink upsert theo
  khóa chính. Độ trễ xấu nhất ≈ trigger + thời gian một batch.

### Chỉ báo có trạng thái

Mỗi micro-batch thường chỉ có khoảng một nến mỗi mã. Bản v1 tính SMA/RSI/MACD/z-score trên
riêng batch đó nên z-score luôn là NaN và **không bao giờ phát cảnh báo**. Có test tái hiện
trong `tests/test_state.py`.

`IndicatorState` giữ 60 nến 1 phút gần nhất cho mỗi mã ở driver (nơi `foreachBatch` chạy).
Nến partial được thay thế theo `window_start`. Khi driver khởi động lại, state được nạp lại
từ `ohlcv_1m` (hai phân vùng `(symbol, day)` gần nhất). Cảnh báo PRICE_SPIKE dùng z-score
so với 30 nến *trước* nến hiện tại (`common.indicators.rolling_zscore`).

### Exactly-once

| Đầu ra | Đảm bảo | Cơ chế |
|---|---|---|
| `ohlcv_1m`, `ohlcv_5m`, `indicators_latest`, `alerts_by_day` | Exactly-once (về giá trị) | Kafka offset trong checkpoint + upsert theo khóa chính; batch chạy lại ghi đè đúng các dòng cũ |
| Volume trong nến | Không cộng hai lần | `dropDuplicatesWithinWatermark(trade_id)`: producer gửi lại sau reconnect bị bỏ |
| Kafka `alerts` | At-least-once | Driver nhớ các `alert_id` đã phát (FIFO có giới hạn); mất danh sách này khi driver khởi động lại, nên bên đọc phải bỏ trùng theo `alert_id` |

## Đo độ trễ và chứng minh exactly-once (việc B.5, B.6)

Metrics Prometheus trên driver, cổng `9108` (`/metrics`), nhãn `query`:

- `market_speed_input_rows_per_second`, `market_speed_processed_rows_per_second`: tốc độ Spark.
- `market_speed_batch_duration_ms`: thời gian chạy một micro-batch.
- `market_speed_offsets_behind_latest`: số tin Kafka chưa đọc. Spark giữ offset trong
  checkpoint chứ không commit vào consumer group, nên kafka-exporter không thấy lag này.
- `market_speed_event_to_sink_latency_seconds`: p95 của `updated_at − last_event_time` trong
  batch gần nhất.

Ngoài ra Spark tự xuất metrics JVM/streaming ở `:4040/metrics/prometheus` (PrometheusServlet).

Đo độ trễ giao dịch → Cassandra và ghi ngay vào nhật ký số liệu:

```bash
python -m streaming.tools.measure_latency --day 2026-11-20 \
  --cassandra-host market-dc1-service.cassandra.svc \
  --note "trigger 5s, 1 executor" --append-metrics docs/metrics/B-speed.md
```

Đo nhanh trên laptop, không cần Kafka/Cassandra:

```bash
python -m streaming.tools.local_latency_run --seconds 60
```

Quy trình chứng minh exactly-once trên cụm:

1. Chạy producer replay một tập file cố định (ví dụ 1 giờ dữ liệu của bronze), đồng thời giữ
   bản sao JSONL của tập đó (`bronze-dump/*.json`).
2. Giữa chừng xoá pod driver:
   `kubectl -n spark delete pod -l spark-role=driver,sparkoperator.k8s.io/app-name=speed-layer`.
   Spark Operator (restartPolicy Always) tạo lại driver; query đọc lại checkpoint
   `s3a://checkpoints/speed-ohlcv-1m/` và chạy lại batch đang dở.
3. Khi replay xong, so nến trong Cassandra với nến tính lại từ dữ liệu gốc:
   `python -m streaming.tools.exactly_once_check --trades 'bronze-dump/*.json' --cassandra-host market-dc1-service.cassandra.svc`
   Exit code 0 nghĩa là không thiếu nến, không có nến lạ, và `trade_count`/`volume` không bị
   cộng hai lần.
4. Ghi kết quả (số lần xoá pod, thời gian phục hồi, kết quả check) vào `docs/metrics/B-speed.md`.

Kịch bản này đã được tự động hoá ở mức local trong
`tests/test_tools.py::test_replayed_micro_batch_is_not_double_counted`: test xoá commit cuối
của checkpoint (đúng trạng thái một driver chết sau khi ghi sink nhưng trước khi commit),
khởi động lại, rồi chạy `exactly_once_check`.

## Test

```bash
JAVA_HOME=/path/to/jdk17 python -m pytest streaming/tests -q
```

- Test thuần (không cần JVM): cột so với CQL, state/chỉ báo/cảnh báo, metrics, công cụ.
- Test Spark local (tự bỏ qua nếu thiếu JVM hoặc pyspark): nến 1m/5m khớp
  `common.ohlcv.aggregate_trades`, upsert nến partial, cảnh báo xuyên batch, payload Kafka
  khớp `alert.schema.json`, replay không đếm trùng.
- PySpark 3.5 vẫn `import distutils` trong `toPandas()`. Trên Python ≥ 3.12 cần cài
  `setuptools` (image Docker đã cài sẵn).

## Giới hạn còn lại

- Chưa chạy với Kafka/Cassandra/MinIO thật. Đường đi đó (connector, s3a, Spark Operator)
  chỉ được kiểm tra bằng kubeconform và đối chiếu hợp đồng; số liệu độ trễ hiện có là đo
  local với nguồn file.
- `updated_at` lấy lúc batch bắt đầu ghi, nên độ trễ đo được nhỏ hơn thời điểm dòng thực sự
  xuất hiện trong Cassandra khoảng bằng thời gian ghi một batch.
- State chỉ báo nằm trong bộ nhớ một driver. Đủ cho khoảng chục mã của đề tài; nếu hàng
  nghìn mã thì nên chuyển sang `applyInPandasWithState` để state nằm trên executor.
- Cảnh báo có thể bắn trên nến partial (giá vọt giữa phút rồi quay lại). Dòng trong
  `alerts_by_day` được upsert với giá trị mới nhất, còn Kafka chỉ nhận lần đầu.
