# Nhật ký bài học — Speed layer (vai B)

Mẫu mỗi mục (PDF ý tưởng, slide "Lessons learned"): vấn đề → các cách đã thử → giải pháp kèm
số liệu → bài học. Ghi ngay khi gặp, không đợi đến cuối kỳ.

## 2026-11-12 — Ghi Cassandra sai cột

- **Vấn đề:** job v1 ghi 21 cột (thêm `ema_12`, `macd_hist`, ...) vào `ohlcv_1m` vốn chỉ có
  15 cột, và ghi cảnh báo thiếu khóa `day`. Spark Cassandra Connector từ chối ngay ở batch đầu.
- **Đã thử:** không có. Lỗi được phát hiện khi đối chiếu với hợp đồng.
- **Giải pháp:** tách ghi theo bảng hợp đồng; `check_columns` chạy lúc runtime;
  `test_contract_columns.py` so với `contracts/cassandra.cql`.
- **Bài học:** hợp đồng phải là file máy đọc được, và test phải đọc chính file đó.

## 2026-11-12 — Cảnh báo không bao giờ bắn

- **Vấn đề:** z-score 30 phút tính trên một micro-batch chỉ có ~1 nến mỗi mã nên luôn NaN.
- **Đã thử:** tăng trigger để batch to hơn. Không đúng bản chất, lại làm tăng độ trễ.
- **Giải pháp:** `IndicatorState` giữ 60 nến/mã qua các batch và nạp lại từ Cassandra khi
  restart. Test tái hiện: 31 batch, mỗi batch 1 nến → v1 không cảnh báo, bản mới có.
- **Bài học:** chỉ báo kỹ thuật là phép tính có trạng thái; streaming phải giữ trạng thái đó
  một cách tường minh.

## 2026-11-12 — Nến vào Cassandra quá chậm

- **Vấn đề:** append mode + watermark 2 phút → nến xuất hiện sau 2–3 phút; PDF yêu cầu < ~10 s.
- **Giải pháp:** update mode, trigger 5 s, upsert theo khóa chính. Đo local: p95 2.1 s, xấu
  nhất ≈ 9.3 s.
- **Bài học:** chọn output mode theo yêu cầu độ trễ, rồi mới tinh chỉnh trigger/watermark.

## 2026-11-12 — PySpark 3.5 trên Python 3.12

- **Vấn đề:** `toPandas()` lỗi `No module named 'distutils'`, vì PySpark 3.5 còn import
  distutils mà Python 3.12 đã bỏ.
- **Giải pháp:** cài `setuptools` (cung cấp shim `distutils`); image Docker đã cài sẵn.
- **Bài học:** ghi rõ phiên bản Python được hỗ trợ cho từng thành phần.

## Cần ghi sau khi chạy cụm

- Kết quả xoá pod driver (thời gian phục hồi, `exactly_once_check`).
- State store phình to khi replay 100x? Đo bộ nhớ driver/executor.
- Hành vi khi broker Kafka chết (`failOnDataLoss=false`, `maxOffsetsBehindLatest`).
