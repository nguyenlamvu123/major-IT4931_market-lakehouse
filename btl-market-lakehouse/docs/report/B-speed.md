# Chương B: Xử lý thời gian thực (Speed layer)

> Viết theo mẫu 4 phần của PDF: vấn đề → giải pháp → số liệu → bài học.
> Số liệu lấy từ `docs/metrics/B-speed.md`; những chỗ ghi *[đo trên cụm]* phải điền sau khi chạy tuần 4–5.

## 1. Vấn đề

- Giao dịch của bốn nhóm tài sản (crypto 24/7, cổ phiếu/ETF Mỹ trong phiên, vàng GC=F, DXY)
  đến liên tục, có thể sai thứ tự, đến muộn, hoặc bị gửi lại khi producer kết nối lại.
- Dashboard cần nến 1 phút/5 phút, VWAP và chỉ báo chậm dưới ~10 giây; cảnh báo khi giá lệch
  hơn 3 độ lệch chuẩn so với 30 phút trước.
- Job phải chịu được việc pod Spark bị xoá giữa chừng mà không ghi trùng dòng nào.

## 2. Giải pháp

- Spark Structured Streaming đọc 4 topic trade; xử lý theo **event time**, watermark 30 s; bỏ
  trùng bằng `dropDuplicatesWithinWatermark(trade_id)`.
- Hai query (1 phút, 5 phút) ở output mode **update**, trigger 5 s; `foreachBatch` upsert vào
  Cassandra theo khóa chính `((symbol, day), window_start)`. Giải thích vì sao không dùng append:
  `streaming/README.md`.
- `IndicatorState` giữ 60 nến gần nhất mỗi mã qua các batch (nạp lại từ Cassandra khi khởi
  động lại) để tính SMA/EMA/MACD/RSI/z-score; ghi `indicators_latest`, `alerts_by_day` và topic
  `alerts`.
- Checkpoint trên MinIO, SparkApplication với `restartPolicy: Always`, metrics Prometheus cổng 9108.
- Kỹ năng Spark của môn (B phụ trách): streaming (window, watermark, state, update mode,
  checkpoint), aggregation (min_by/max_by, sum, count, VWAP), pandas trên driver cho chỉ báo
  (vai trò của UDF).

## 3. Số liệu

| Chỉ số | Local (đã đo) | Cụm *[đo trên cụm]* |
|---|---|---|
| Độ trễ trade → nến 1m, p95 | 2.10 s (nguồn file) | |
| Độ trễ xấu nhất ước tính | ≈ 9.3 s | |
| Tốc độ xử lý | ~940 dòng/s (local[2]) | |
| Replay sau khi chết giữa batch | 0 nến trùng/lệch | |
| Thời gian phục hồi sau khi xoá pod driver | — | |

## 4. Bài học

Xem `docs/lessons/B-speed.md`. Tóm tắt:

- Phải đối chiếu cột ghi với CQL bằng test, nếu không lỗi chỉ lộ ra trên cụm.
- Chỉ báo cần trạng thái liên tục; tính trên từng micro-batch là sai về bản chất.
- Muốn đạt "dưới 10 s" thì output mode quan trọng hơn mọi tinh chỉnh khác.
