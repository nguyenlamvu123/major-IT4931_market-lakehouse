# Kafka Connect S3 sink → MinIO bronze (vai A)

Kafka Connect là "người đọc" thứ hai của Kafka (PDF, hành trình giao dịch bước 2). Nó chép
nguyên trạng mọi topic đầu vào sang MinIO để C xử lý batch.

| Connector | Topic | Phân vùng `dt` theo | Consumer group |
|---|---|---|---|
| `s3-sink-bronze` | `crypto.trades`, `stocks.trades`, `stocks.bars`, `gold.quotes`, `fx.dxy`, `rates.yields` | trường `event_time` của bản ghi (UTC) | `connect-s3-sink-bronze` |
| `s3-sink-bronze-alerts` | `alerts` | timestamp của bản ghi Kafka (lúc B ghi) | `connect-s3-sink-bronze-alerts` |

`alerts` tách thành connector riêng vì bản ghi alert không có `event_time` (`alert_time` là
chuỗi ISO), mà một connector chỉ đọc được một trường thời gian.

Kết quả trên MinIO (khớp `contracts/paths.yaml`):

```text
s3a://lake/bronze/<topic>/dt=YYYY-MM-DD/<topic>+<partition>+<startOffset>.json   # JSON lines
```

## Các lỗi của bản v1 đã sửa

| Lỗi | Hậu quả | Sửa |
|---|---|---|
| `TimeBasedPartitioner` thiếu `partition.duration.ms`, `locale`, `timezone` | Connector không qua được bước validate cấu hình | Khai báo đủ: 1 ngày, `en-US`, `UTC` |
| `timestamp.extractor=Record` + `timestamp.field` | `timestamp.field` bị bỏ qua, dữ liệu replay rơi vào `dt` của ngày gửi | `RecordField` + `event_time` |
| Thiếu `storage.class` | Lỗi cấu hình bắt buộc | `io.confluent.connect.s3.storage.S3Storage` |
| `s3.path.style.access` (sai tên) | Bị bỏ qua | `s3.path.style.access.enabled` |
| `register_connector.sh` PUT cả `{name, config}` vào `/connectors/<name>/config` | Connect từ chối (thiếu `connector.class`) | Chỉ gửi map `config` |
| Phân vùng `dt=/hour=` | Lệch hợp đồng `bronze/<topic>/dt=YYYY-MM-DD/` | Bỏ `hour` |

## Đánh đổi khi chọn cách xoay file

- `flush.size=1000` và `rotate.interval.ms=60000` (theo thời gian bản ghi) tạo file với các
  topic nhiều tin.
- `rotate.schedule.interval.ms=300000` (theo đồng hồ) là bắt buộc với topic thưa như
  `rates.yields`, vốn chỉ có vài tin mỗi ngày; thiếu nó thì file nằm trong bộ đệm cả ngày.
- Theo tài liệu Confluent, xoay theo đồng hồ làm mất tính exactly-once của S3 sink: khi task
  khởi động lại, một khoảng dữ liệu có thể được ghi hai lần. Silver (C) bỏ trùng theo
  `trade_id`/`bar_id`/(`series_id`, `day`), nên chấp nhận được.

## Kubernetes (Strimzi)

```bash
# 1) Image có plugin S3. Phiên bản Strimzi/Kafka phải khớp operator do D cài.
docker buildx build --platform linux/arm64 -f ingest/kafka_connect/Dockerfile \
  -t btl/kafka-connect-s3:dev ingest/kafka_connect
minikube image load btl/kafka-connect-s3:dev

# 2) Cần secret minio-credentials trong namespace kafka (D: make secrets)
kubectl apply -k ingest/k8s/kafka-connect
kubectl -n kafka get kafkaconnect,kafkaconnector
```

Các file `ingest/k8s/kafka-connect/s3-sink-*.yaml` sinh ra từ file JSON ở đây.
`ingest/tests/test_ingest_kafka_connect.py` kiểm tra hai bên luôn giống nhau (chỉ khác
`store.url`).

## Docker Compose

```bash
docker build -f ingest/kafka_connect/Dockerfile \
  --build-arg BASE_IMAGE=confluentinc/cp-kafka-connect:7.7.1 \
  --build-arg PLUGIN_PATH=/usr/share/confluent-hub-components \
  --build-arg RUNTIME_USER=appuser -t btl/kafka-connect-s3:compose ingest/kafka_connect
CONNECT_URL=http://localhost:8083 bash ingest/kafka_connect/register_connector.sh
```

Worker cần biến `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (khóa MinIO) và topic
`dead-letter` phải tồn tại trước (`make topics`).

## Kiểm tra

```bash
curl -s localhost:8083/connectors/s3-sink-bronze/status
mc ls -r local/lake/bronze | head
```
