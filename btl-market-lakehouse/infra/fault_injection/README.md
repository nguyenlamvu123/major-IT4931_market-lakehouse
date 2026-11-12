# Thử lỗi (PDF việc D.6)

Mục đích: cố ý làm hỏng từng thành phần, rồi ghi lại hệ thống phản ứng thế nào. Kết quả
dùng cho phần *bài học kinh nghiệm*, mảng 7 (giám sát) và mảng 11 (chịu lỗi) trong slide
"Lessons learned". Ghi kết quả vào `docs/lessons/D-fault-injection.md` ngay khi đo.

## Kubernetes (`k8s/`, cụm dựng bằng `make up`)

| # | Script | Làm gì | Kỳ vọng |
|---|---|---|---|
| 1 | `k8s/kill_kafka_broker.sh` | Xoá pod broker Strimzi | Dev (1 broker): producer retry rồi chạy tiếp, không mất dữ liệu nhờ `acks=all` + idempotent. Demo (3 broker RF3 ISR2): không gián đoạn |
| 2 | `APP=speed-layer k8s/kill_spark_driver.sh` | Xoá driver Spark Streaming | Spark Operator tạo driver mới, đọc tiếp từ checkpoint MinIO, Cassandra không có dòng trùng (exactly-once, việc B.5) |
| 3 | `APP=speed-layer k8s/kill_spark_executor.sh` | Xoá executor | Task chạy lại, micro-batch dài hơn một nhịp |
| 4 | `k8s/kill_cassandra.sh` | Xoá pod Cassandra | market-api trả 503, ô "Nguồn dữ liệu" = none; streaming/batch retry; tự hồi phục |
| 5 | `k8s/stop_minio.sh`, sau đó `k8s/restore.sh` | Tắt MinIO | Lag của S3 sink tăng; Kafka giữ dữ liệu 7 ngày nên không mất; streaming dừng ghi checkpoint |
| 6 | `SPEED=100 k8s/flood_replay.sh`, sau đó `k8s/restore.sh` | Replay 100x | Tìm throughput tối đa; offsets chưa đọc tăng khi vượt sức xử lý |

Mỗi script in ra:

- giờ UTC bắt đầu và thời gian phục hồi (giây);
- panel Grafana cần nhìn (dashboard "Market Lakehouse: giám sát hệ thống");
- số liệu cần ghi lại.

Tên Deployment replay và biến tốc độ do vai A đặt. Nếu khác mặc định, ghi đè bằng
`REPLAY_DEPLOYMENT=... REPLAY_SPEED_ENV=...`.

## Docker Compose (stack `docker-compose.full.yml`)

| Script | Làm gì |
|---|---|
| `stop_cassandra.sh` | Dừng Cassandra |
| `stop_minio.sh` | Dừng MinIO |
| `stop_spark_worker.sh` | Dừng Spark worker |
| `pause_consumer.sh` | Dừng Kafka Connect: lag tăng |
| `restore.sh` | Khởi động lại tất cả |

Trên compose, market-api chạy với `DEMO_FALLBACK=true`, nên khi Cassandra tắt, API
chuyển sang dữ liệu demo. Header `X-Data-Source: demo` và ô "Nguồn dữ liệu" cho biết điều
đó.

## Cách ghi kết quả

Mẫu 4 phần của PDF: vấn đề → các cách đã thử → giải pháp kèm số liệu → bài học. Mỗi lần
thử ghi một dòng vào bảng "Kết quả" của `docs/lessons/D-fault-injection.md`, gồm:

- ngày và cấu hình (số broker, số executor);
- thời gian phát hiện (từ lúc chạy script tới khi Grafana đổi màu hoặc vượt ngưỡng);
- thời gian phục hồi;
- số message mất, số dòng trùng;
- có cần thao tác tay hay không.
