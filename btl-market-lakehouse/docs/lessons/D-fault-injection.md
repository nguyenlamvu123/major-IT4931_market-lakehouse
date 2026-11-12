# Nhật ký thử lỗi (vai D)

PDF việc D.6: "tắt broker Kafka, xoá executor Spark, ghi lại hệ thống phản ứng thế nào".
Kịch bản và lệnh: `infra/fault_injection/README.md`. Ghi ngay khi đo, không để đến cuối kỳ.

## Kịch bản

| # | Sự cố | Lệnh | Quan sát trên Grafana | Phục hồi | Số liệu cần ghi |
|---|---|---|---|---|---|
| 1 | Broker Kafka chết | `infra/fault_injection/k8s/kill_kafka_broker.sh` | Producer: lỗi/kết nối lại; Tin nhắn vào Kafka/giây; Pod khởi động lại | Strimzi tạo lại pod | Thời gian phục hồi; message mất = `market_producer_messages_total` so với số dòng bronze cùng khoảng |
| 2 | Driver Spark bị kill | `APP=speed-layer infra/fault_injection/k8s/kill_spark_driver.sh` | Spark: dòng xử lý/giây, offsets chưa đọc; Độ trễ giao dịch → Cassandra | `restartPolicy: Always` + checkpoint MinIO | Thời gian phục hồi; số dòng trùng trong `ohlcv_1m` (phải = 0) |
| 3 | Executor Spark mất | `APP=speed-layer infra/fault_injection/k8s/kill_spark_executor.sh` | Thời gian micro-batch; CPU theo pod | Driver xin executor mới | Micro-batch dài nhất (ms) |
| 4 | Cassandra chết | `infra/fault_injection/k8s/kill_cassandra.sh` | Ô Nguồn dữ liệu = none (503); market-api p95 | cass-operator dựng lại pod | Thời gian phục hồi; dashboard có tự hồi phục không |
| 5 | MinIO ngừng | `infra/fault_injection/k8s/stop_minio.sh` → `restore.sh` | Kafka consumer lag (`connect-s3-sink-bronze*`) | Bật lại MinIO, Connect tự retry | Lag lớn nhất; thời gian lag về 0; file bronze thiếu hoặc trùng |
| 6 | Replay 100x vượt sức | `SPEED=100 infra/fault_injection/k8s/flood_replay.sh` → `restore.sh` | Tin nhắn vào/giây; Spark vào > xử lý; offsets chưa đọc; RAM | Tăng partition/executor hoặc hạ tốc độ | Throughput tối đa ổn định (msg/s) theo số executor |
| 7 | Máy ngủ, operator mất lease (đã gặp, slide "Sự cố về hệ thống") | — | Pod khởi động lại (operator) | Operator tự khởi động lại; giữ máy thức (`caffeinate -dims`) | Số lần xảy ra; thời gian cụm ổn định lại |

## Kết quả

| Ngày (UTC) | Kịch bản | Cấu hình | Phát hiện sau (s) | Phục hồi sau (s) | Message mất | Dòng trùng | Thao tác tay | Ghi chú |
|---|---|---|---|---|---|---|---|---|
| _chưa đo_ | | | | | | | | |

## Bài học (điền sau khi đo)

Theo mẫu 4 phần: **vấn đề** → **các cách đã thử** → **giải pháp kèm số liệu** → **bài học**.
