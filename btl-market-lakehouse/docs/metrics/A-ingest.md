# Số liệu đo: vai A

Ghi ngay khi đo (PDF mục 9: "Ghi ngay khi đo vào docs/metrics … ngày, cấu hình, kết quả").

| Ngày | Chỉ số | Cấu hình | Kết quả | Lệnh |
|---|---|---|---|---|
| 2026-11-12 | Trần đường publish của 1 producer (route + validate + dedupe + Prometheus, sink rỗng, không Kafka) | Laptop Intel i5-13400 (16 luồng), Python 3.12, 99.900 trade giả lập, 3 lần chạy | 202.842 / 243.522 / 204.159 bản ghi/s (trung vị ≈ 204 nghìn/s) | `python -m ingest.tools.bench_publish --rows 100000` |
| … | Tin/giây mỗi topic trên broker, trong phiên Mỹ | minikube, 1 broker | … | `python -m ingest.tools.topic_rates --interval 60 --samples 60` |
| … | 4 nguồn chạy 1 giờ liền | minikube | restarts = …, errors = … | `kubectl -n market get pods`, Grafana |
| … | % trùng / % đến muộn / độ trễ thu thập p95 | dump 1 giờ | … | `python -m ingest.tools.feed_quality` |

Nhận xét về số liệu benchmark: ngay cả khi replay 100x, lưu lượng chỉ cỡ vài nghìn tin/giây.
Con số này nhỏ hơn trần đường publish khoảng hai bậc, nên nếu replay bị nghẽn thì nguyên
nhân nằm ở Kafka, mạng hoặc Spark, không phải ở producer.
