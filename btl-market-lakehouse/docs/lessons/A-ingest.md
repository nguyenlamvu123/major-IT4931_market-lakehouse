# Nhật ký sự cố: vai A (thu thập dữ liệu và Kafka)

> PDF ý tưởng: "Ghi nhật ký sự cố vào docs/lessons/ ngay khi gặp, không để đến cuối kỳ".
> Mỗi mục theo mẫu: vấn đề → các cách đã thử → giải pháp kèm số liệu → bài học.

## Mẫu

```text
### YYYY-MM-DD: <tiêu đề ngắn>
- Hiện tượng:
- Ảnh hưởng (topic, khoảng thời gian, số bản ghi):
- Phát hiện bằng (log / Grafana / test):
- Các cách đã thử:
- Giải pháp + số liệu trước/sau:
- Bài học:
```

## 2026-11-12: Poll Alpaca REST lặp lại 100 trade đầu ngày

- Hiện tượng: poller chỉ gửi `limit=100`. Alpaca mặc định `start` là đầu ngày và trả kết
  quả tăng dần, nên lần poll nào cũng nhận đúng 100 trade đó. Cả 100 trade đã nằm trong
  tập `_seen`, nên không có tin mới nào lên Kafka. Tài khoản miễn phí còn có thể bị từ
  chối vì thiếu `feed=iex`.
- Ảnh hưởng: chế độ REST của `stocks.trades` không bao giờ có dữ liệu mới.
- Phát hiện bằng: rà soát code đối chiếu với PDF.
- Giải pháp: con trỏ `start` theo từng mã, thêm `feed=iex`, đi theo `next_page_token`, lọc
  trùng ở biên bằng `trade_id`. Đồng thời thay tập `_seen` vô hạn bằng LRU có giới hạn.
  Có test tái hiện với server Alpaca giả.
- Bài học: poller nào cũng cần con trỏ thời gian và một test "lần poll thứ hai chỉ trả tin
  mới".

## 2026-11-12: S3 sink không khởi động được

- Hiện tượng: `TimeBasedPartitioner` thiếu `partition.duration.ms`, `locale`, `timezone`;
  thiếu `storage.class`; `timestamp.extractor=Record` khiến `timestamp.field` bị bỏ qua;
  sai tên `s3.path.style.access`; `register_connector.sh` PUT cả `{name, config}`.
- Ảnh hưởng: bronze rỗng, nên toàn bộ batch layer của C không có đầu vào.
- Giải pháp: cấu hình đầy đủ, dùng `RecordField` trên `event_time`, tách connector riêng
  cho `alerts` (không có `event_time`), thêm test so cấu hình với `contracts/`.
- Bài học: cấu hình hạ tầng cũng cần test tự động như code.

## 2026-11-12: `.dockerignore` không có tác dụng

- Hiện tượng: file nằm ở `ingest/.dockerignore`, nhưng build context là gốc dự án, nên
  Docker không đọc file này. Image vì thế kéo theo cả `tests/`, `data/` và có thể cả `.env`.
- Giải pháp: chuyển thành `ingest/Dockerfile.dockerignore` (BuildKit đọc file đặt cạnh
  Dockerfile).
