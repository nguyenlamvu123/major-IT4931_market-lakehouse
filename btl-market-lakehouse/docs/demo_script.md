# Kịch bản demo (khoảng 15 phút)

Phiên chính NYSE/Nasdaq là 9:30–16:00 giờ New York, tức khoảng 21:30–04:00 giờ Việt Nam.
Buổi thuyết trình ban ngày nên **thị trường Mỹ đóng cửa**. Vì vậy:

- cổ phiếu, vàng, ETF trái phiếu chạy bằng **producer replay**;
- crypto vẫn chạy **live 24/7** từ Binance.

Bản này có thể chỉnh trực tiếp. Tên Deployment replay và biến tốc độ do vai A đặt trong
`ingest/k8s`; nếu khác mặc định thì đổi `REPLAY_DEPLOYMENT` / `REPLAY_SPEED_ENV`.

## Chuẩn bị (trước giờ thuyết trình 1 tiếng)

```bash
caffeinate -dims &          # macOS: máy ngủ làm operator mất lease (đã gặp)
make up                     # nếu cụm chưa chạy
make status                 # mọi pod Running, kafka/market Ready, job schema Complete
make port-forward &         # Grafana :3000, API :8000, Airflow :8080, Prometheus :9090
REQUIRE_REAL_DATA=1 bash scripts/smoke_test.sh
```

- Mở sẵn các tab: Grafana "Market Lakehouse: thị trường", Grafana "Market Lakehouse: giám
  sát hệ thống", Airflow (DAG của C), MinIO console (bucket `lake`).
- Airflow đã chạy DAG ít nhất một đêm, để bảng tương quan và đường cong lợi suất có số liệu.
- Video dự phòng (việc D.7) quay theo đúng kịch bản này, để trên máy và trên Drive.

## Diễn biến

| Phút | Người nói | Màn hình | Nội dung |
|---|---|---|---|
| 0–2 | D | Slide sơ đồ (`docs/architecture.md`) | Lambda: một luồng vào Kafka, nhánh nhanh (giây) và nhánh chậm (đêm), gặp nhau ở Cassandra, tất cả trên K8s |
| 2–4 | A | `kubectl -n market get pods`; panel "Tin nhắn vào Kafka / giây" | Producer Binance live; bật replay cổ phiếu: `SPEED=10 REPLICAS=1 bash infra/fault_injection/k8s/flood_replay.sh` (giả lập phiên Mỹ ở tốc độ 10x) |
| 4–7 | B | Dashboard thị trường: nến 1m BTCUSDT, đổi `symbol` sang SPY | Nến cập nhật vài giây một lần; VWAP; bảng chỉ báo; giải thích watermark; panel "Độ trễ giao dịch → Cassandra" dưới 10 s |
| 7–8 | B | Bảng "Cảnh báo trong ngày" | Cảnh báo z-score > 3 so với 30 phút trước (replay tăng tốc dễ tạo biến động) |
| 8–11 | C | Airflow (DAG, log), MinIO `bronze/…/dt=`, dashboard: so sánh tài sản, tương quan 90 ngày, đường cong lợi suất | bronze → silver → gold; trả lời 2–3 câu hỏi phân tích (TLT với US10Y, GLD với REAL10Y, spread 10Y-2Y); kết quả MLlib |
| 11–13 | D | Dashboard giám sát | Kafka lag, Spark vào/xử lý, CPU/RAM. Thử lỗi trực tiếp: `APP=speed-layer bash infra/fault_injection/k8s/kill_spark_driver.sh`, xem driver mới lên, độ trễ trở lại dưới 10 s, không có dòng trùng |
| 13–15 | Cả nhóm | Slide lessons learned | Mỗi vai một bài học kèm số liệu từ `docs/metrics.md` |

Sau demo: `bash infra/fault_injection/k8s/restore.sh` để tắt replay.

## Nếu có sự cố khi demo

| Sự cố | Xử lý trong 30 giây |
|---|---|
| Grafana trống, ô Nguồn dữ liệu = none | Cassandra đang khởi động lại: chuyển sang dashboard giám sát và giải thích cơ chế 503 (không hiện số giả) |
| Binance không vào được (mạng hội trường) | Replay cả crypto: chạy `flood_replay.sh` với file crypto (vai A) |
| Cụm không lên | Chiếu video dự phòng; chạy `make demo` + `make api` trên laptop để vẫn trả lời câu hỏi về logic |
