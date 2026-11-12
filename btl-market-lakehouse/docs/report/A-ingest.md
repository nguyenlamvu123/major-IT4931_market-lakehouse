# Chương A: Thu thập dữ liệu và Kafka

> Viết theo mẫu 4 phần của PDF "Kiến trúc – Phân công" (mục 5, việc chung): vấn đề → giải
> pháp → số liệu → bài học. Ô `…` là chỗ điền sau khi đo trên cụm. Đo xong ghi ngay vào
> `docs/metrics/A-ingest.md`, rồi chép sang đây.

## 1. Vấn đề

- Bốn nhóm tài sản đến từ nguồn khác nhau, mỗi nguồn một giao thức, một định dạng và một
  nhịp dữ liệu:
  - crypto: Binance WebSocket, 24/7;
  - cổ phiếu Mỹ và ETF GLD/TLT/IEF/SHY: Alpaca IEX WebSocket, chỉ trong phiên;
  - vàng GC=F và chỉ số USD: Yahoo, poll;
  - lợi suất trái phiếu: Treasury CSV hoặc FRED, mỗi ngày một lần.
- B và C cần một định dạng duy nhất từ tuần 1, trước khi có dữ liệu thật.
- Phiên Mỹ diễn ra 21:30–04:00 giờ Việt Nam, nên ban ngày không có dữ liệu sống để demo.
- Kết nối hay rớt, API giới hạn tần suất, và sau khi kết nối lại thì tin bị gửi trùng.

## 2. Giải pháp

| Thành phần | Quyết định | Lý do |
|---|---|---|
| Hợp đồng v2 | 7 topic theo PDF, schema JSON, 20 dòng mẫu mỗi topic, `trade_id` tất định | B/C code song song từ tuần 1 |
| Định tuyến | Topic theo feed, `asset_class` theo tài sản gốc | GLD qua Alpaca nhưng vẫn là "vàng" khi phân tích |
| Key Kafka | `symbol` (lợi suất: `tenor`) | Giữ thứ tự theo từng mã trong một partition |
| Producer | Idempotent, `acks=all`, validate trước khi gửi, có dead-letter | Không mất tin, không âm thầm bỏ tin |
| Mất kết nối | Exponential backoff + jitter; circuit breaker cho REST | Không dội API khi nguồn đang lỗi |
| Trùng tin | Id của sàn + LRU trong tiến trình; silver bỏ trùng lần nữa | Hai lớp bảo vệ |
| Demo ban ngày | Replay 1x–100x từ bronze, `--rebase-time` | Speed layer coi như dữ liệu sống |
| Lịch sử | Backfill nến ngày yfinance nhiều năm vào `stocks.bars` | Tương quan 90 ngày có dữ liệu ngay |
| Lưu thô | Kafka Connect S3 sink → `bronze/<topic>/dt=…/` | C đọc lại toàn bộ mỗi đêm |
| Triển khai | Image `btl/ingest:dev`, Deployment mỗi nguồn, ServiceMonitor | "Chạy 1 giờ liền trên K8s, có số liệu tin/giây" |

## 3. Số liệu

| Chỉ số | Cách đo | Kết quả | Ghi chú |
|---|---|---|---|
| Tin/giây mỗi topic (broker) | `python -m ingest.tools.topic_rates --interval 60 --samples 60` | … | Trong phiên Mỹ và ngoài phiên |
| 4 nguồn chạy 1 giờ liền trên K8s | `kubectl -n market get pods` (restarts), `market_producer_errors_total` | … | Điều kiện "Xong" của PDF |
| % trùng | `ingest.tools.feed_quality` trên dump 1 giờ | … | Trước và sau khi bỏ trùng trong tiến trình |
| % đến muộn (> 30 s) | `ingest.tools.feed_quality --late-after-ms 30000` | … | Chính là phần speed layer phải bỏ qua |
| Độ trễ thu thập p50/p95 (`ingest_time − event_time`) | `feed_quality` | … | |
| Số lần kết nối lại / giờ | `increase(market_producer_reconnects_total[1h])` | … | |
| Số bản ghi vào dead-letter | `market_producer_dead_letter_total` | … | |
| Trần đường publish của 1 pod (không có Kafka) | `python -m ingest.tools.bench_publish --rows 100000` | ~200 nghìn bản ghi/s (đo local, xem metrics) | Producer không phải nút cổ chai khi replay 100x |
| Replay 10x / 100x: tin/giây đạt được | `topic_rates` khi bật `producer-replay` | … | Dùng cho phần "Mở rộng" |

## 4. Bài học

Ghi theo nhật ký `docs/lessons/A-ingest.md`. Đã có sẵn các bài học từ đợt rà soát
12/11/2026:

1. Poll REST mà không có con trỏ thời gian thì "chạy được" nhưng không bao giờ ra dữ liệu
   mới (Alpaca). Mọi poller cần một `start` cursor và một test với server giả.
2. Chỉ test parser thì không phát hiện được lỗi cấu hình hạ tầng. Cấu hình S3 sink thiếu
   khóa bắt buộc nằm im cho đến khi có test so cấu hình với hợp đồng.
3. Tên topic phải nằm ở một nơi duy nhất (`common/contracts.py`). Bản v1 có 3 bộ tên
   topic khác nhau trong code, tài liệu và PDF.
4. …
