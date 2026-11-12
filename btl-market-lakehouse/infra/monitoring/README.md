# Giám sát và dashboard (vai D)

PDF việc D.5 yêu cầu hai dashboard:

- **Dashboard thị trường** (`market-overview.json`):
  - Nến 1 phút realtime kèm VWAP, nến 5 phút.
  - Bảng chỉ báo mới nhất, cảnh báo trong ngày.
  - So sánh nhiều tài sản (giá đóng cửa quy về 100 ở ngày đầu).
  - Ma trận tương quan 90 ngày, đường cong lợi suất, spread 10Y-2Y.
  - Phân cụm MLlib, và một ô "Nguồn dữ liệu" cho biết đang xem dữ liệu thật hay demo.
  - Đọc `market-api` qua plugin Infinity.
- **Dashboard giám sát** (`infra-health.json`):
  - Kafka consumer lag, tin nhắn vào mỗi giây theo topic.
  - Producer: msg/s, lỗi, kết nối lại, dead-letter.
  - Spark streaming: dòng vào/xử lý mỗi giây, offsets chưa đọc, thời gian micro-batch,
    độ trễ từ giao dịch đến Cassandra (ngưỡng 10 s).
  - CPU/RAM từng pod, RAM còn trống của máy, số lần pod khởi động lại, p95 của market-api.
  - Nguồn: Prometheus.

## Sinh dashboard từ code

Hai file JSON được sinh bởi `infra/scripts/build_dashboards.py`. Không sửa tay; sửa
script rồi chạy:

```bash
make dashboards        # python -m infra.scripts.build_dashboards
python -m pytest infra/tests/test_dashboards.py
```

Test bắt các lỗi sau:

- File JSON lệch so với script.
- URL Infinity không có trong market-api, hoặc thiếu cột mà panel đọc (test gọi thật
  từng URL lên API ở chế độ demo).
- Metric `market_*` chưa khai báo trong `contracts/infra.yaml`.

## Nguồn metric

| Metric | Nguồn | Cách thu |
|---|---|---|
| `kafka_consumergroup_lag`, `kafka_topic_partition_current_offset` | kafkaExporter của Strimzi (K8s), `kafka-exporter` (compose) | PodMonitor `kafka-exporter` |
| `market_producer_*` | Producer vai A, cổng 8001 | PodMonitor `market-workloads` (label `btl.market/metrics=true`, port tên `metrics`) |
| `market_speed_*` | Driver streaming vai B, cổng 9108 | PodMonitor `market-workloads` |
| `market_api_*`, `process_*` | market-api | ServiceMonitor `market-api` |
| `container_*`, `kube_pod_*`, `node_memory_*` | cAdvisor, kube-state-metrics, node-exporter (kube-prometheus-stack) | Chart tự cấu hình |

Spark Structured Streaming lưu offset trong checkpoint trên MinIO, không commit vào
Kafka. Vì vậy `kafka_consumergroup_lag` chỉ đo được lag của Kafka Connect; lag của speed
layer xem ở `market_speed_offsets_behind_latest`.

## Chạy

- Compose: `docker compose -f infra/docker-compose.full.yml up -d`, rồi mở Grafana tại
  http://localhost:3000 (admin/admin), thư mục "Market Lakehouse". Datasource được cấu
  hình trong `grafana/provisioning/`.
- K8s: `make up`. kube-prometheus-stack cài plugin Infinity và datasource `Market API`.
  Sidecar nạp ConfigMap `grafana-dashboards-market` (sinh bằng kustomize từ chính hai
  file JSON này).

Datasource dùng uid cố định `prometheus` và `infinity` ở cả hai môi trường, và URL
Infinity là đường dẫn tương đối, nên cùng một file JSON chạy được cả trên compose lẫn K8s.
