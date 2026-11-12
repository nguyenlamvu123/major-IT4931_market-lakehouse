# Số liệu đo đạc

PDF mục 9: "Ghi ngay khi đo vào docs/metrics.md (ngày, cấu hình, kết quả). Cuối kỳ không
phải đo lại." Mỗi dòng gồm **ngày (UTC)**, **cấu hình** (máy, số broker/executor, tốc độ
replay…) và **kết quả**. Số liệu chi tiết của từng vai có thể nằm trong
`docs/metrics/<vai>.md` trên nhánh của vai đó; bảng dưới đây là bản tổng hợp cho báo cáo.

## Chỉ tiêu cần đo (theo thẻ việc trong PDF)

| Vai | Chỉ tiêu | Định nghĩa | Mục tiêu | Lấy số ở đâu |
|---|---|---|---|---|
| A | Thông lượng mỗi topic | tin nhắn/giây vào Kafka | Có số cho cả 4 nguồn | Panel "Tin nhắn vào Kafka / giây theo topic", `market_producer_messages_total` |
| A | Độ ổn định | 4 nguồn chạy 1 giờ liền trên K8s, không lỗi | 0 restart | `market_producer_errors_total`, Pod khởi động lại |
| A | Tin trùng / dead-letter | % bản ghi bị bỏ trùng; số bản ghi vào `dead-letter` | Ghi rõ | `market_producer_duplicates_total`, `market_producer_dead_letter_total` |
| B | Độ trễ end-to-end | từ `event_time` đến khi ghi `ohlcv_1m` (`updated_at - last_event_time`) | dưới ~10 s | Panel "Độ trễ giao dịch → Cassandra" |
| B | Exactly-once | số dòng trùng sau khi xoá driver giữa chừng | 0 | Kịch bản thử lỗi #2 + công cụ đếm của B |
| B | Tốc độ xử lý | dòng xử lý/giây, thời gian micro-batch | Xử lý ≥ dòng vào | `market_speed_*` |
| C | Tối ưu hiệu năng | thời gian job trước/sau: số partition, broadcast join, cache, AQE | Bảng trước/sau | `docs/metrics/C.md` trên nhánh C |
| C | DAG ổn định | 3 đêm liên tiếp chạy thành công | 3/3 | Giao diện Airflow |
| D | Dựng cụm | `make up` trên máy sạch | Thành công, ghi thời gian | Log của `make up` |
| D | Thử lỗi | thời gian phát hiện/phục hồi, message mất/trùng | Ghi đủ 6 kịch bản | `docs/lessons/D-fault-injection.md` |
| D | market-api | p95 độ trễ | dưới 300 ms trên cụm | Panel "market-api: p95" |
| D | Tài nguyên | RAM/CPU toàn cụm lúc đủ tải | Vừa 16 GB của minikube | Panel CPU/RAM theo pod, RAM còn trống |

## Kết quả

### A: Thu thập dữ liệu

| Ngày | Cấu hình | Kết quả |
|---|---|---|
| _chưa đo trên cụm_ | | |

### B: Speed layer

| Ngày | Cấu hình | Kết quả |
|---|---|---|
| _chưa đo trên cụm_ | | |

### C: Batch layer

| Ngày | Cấu hình | Kết quả |
|---|---|---|
| _xem `docs/metrics/C.md` trên nhánh C_ | | |

### D: Hạ tầng và dashboard

| Ngày | Cấu hình | Kết quả |
|---|---|---|
| 2026-11-12 | Laptop i5-13400 (16 luồng); không Docker/cluster; Python 3.12 venv | Test của nhánh D xanh (`python -m pytest`); `kustomize build` + kubeconform: 36/37 resource hợp lệ, 1 bỏ qua (K8ssandraCluster chưa có schema công khai) |
| 2026-11-12 | Như trên; market-api ở chế độ demo, gọi trong tiến trình qua TestClient, 200 lần mỗi endpoint | Khởi động nguội (dựng dữ liệu demo) 0,67 s. p50/p95 (ms): ohlcv 5,8/7,5; indicators 4,6/6,1; alerts 4,8/6,1; daily/compare 12,4/20,1; correlation matrix 7,4/20,0; yield-curve 365 ngày 5,9/20,3. Chưa gồm mạng và Cassandra |
| _chưa đo_ | minikube 6 CPU/16 GB trên MacBook M1 Max | Thời gian `make up` trên máy sạch; RAM toàn cụm lúc đủ tải |
