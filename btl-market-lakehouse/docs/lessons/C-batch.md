# Nhật ký bài học – vai C

Theo PDF ý tưởng ("Lessons learned"): mỗi mục ghi **vấn đề → các cách đã thử → giải pháp kèm
số liệu → bài học**, ghi ngay khi gặp.

## 2026-11-12 – Không đọc được cả thư mục bronze trong một lần

- **Vấn đề:** job v1 gọi `spark.read.json("s3a://lake/bronze")` và lỗi
  `AssertionError: Conflicting directory structures detected`.
- **Đã thử:** đọc bằng schema cố định thì vẫn lỗi, vì lỗi nằm ở bước phát hiện phân vùng
  chứ không ở bước suy schema.
- **Giải pháp:** đọc từng topic `bronze/<topic>/dt=<ngày>/` (chỉ những ngày có tồn tại,
  kiểm tra qua Hadoop FS) và lấy `_bronze_dt` từ `input_file_name()`. Có test tái hiện
  (`test_reading_bronze_root_in_one_call_fails`).
- **Bài học:** layout do Kafka Connect quyết định (`topics.dir/<topic>/dt=`), nên phải
  thử đọc đúng layout đó ngay từ tuần 1.

## 2026-11-12 – Job gold v1 group theo cột không tồn tại

- **Vấn đề:** `silver_to_gold.py` đọc silver *trades* nhưng group theo
  `window_start/open/close` của nến, nên lỗi `UNRESOLVED_COLUMN` trước khi ghi được gì.
- **Giải pháp:** tổng hợp thẳng từ trades (`min_by`/`max_by` theo `event_time_ms`), hợp
  với nến theo thứ tự ưu tiên nguồn. Kết quả được đối chiếu với `common.analytics`.
- **Bài học:** job Spark không có test chạy thật thì không biết nó hỏng; mỗi job cần ít
  nhất một test Spark local trên dữ liệu mẫu.

## 2026-11-12 – Timestamp trả về Python là giờ địa phương

- **Vấn đề:** kiểm tra độ tươi của crypto so `max(event_time)` với cuối ngày UTC, nhưng
  PySpark trả `datetime` không có múi giờ, theo giờ máy (Asia/Ho_Chi_Minh), nên lệch 7 giờ.
- **Giải pháp:** lấy `max(event_time_ms)` rồi tự đổi sang UTC. Session Spark ghim
  `spark.sql.session.timeZone=UTC` để `to_date` đúng ngày UTC.
- **Bài học:** giữ epoch mili giây trong silver (`event_time_ms`) cạnh cột timestamp.

## 2026-11-12 – Cột được thay thế bị resolve lại

- **Vấn đề:** `withColumn("event_time", timestamp_millis("event_time"))` rồi
  `withColumn("dt", to_date(<biểu thức cũ>))` lỗi `DATATYPE_MISMATCH`: biểu thức cũ được
  resolve lại trên cột *mới* (đã là timestamp).
- **Giải pháp:** sau khi thay cột thì dùng `to_date("event_time")`.
- **Bài học:** biểu thức Column của PySpark là lazy và được resolve theo tên tại DataFrame
  đích.

## 2026-11-12 – PySpark 3.5 trên Python 3.12

- **Vấn đề:** `toPandas()`, `createDataFrame(pandas)` và `pyspark.ml` import `distutils`,
  mà Python 3.12 đã bỏ module này.
- **Giải pháp:** test thu dữ liệu bằng `collect()`; test MLlib tự skip khi thiếu
  `pyspark.ml`. Image `btl/batch:dev` dùng Python 3.10, nên không bị ảnh hưởng.
- **Bài học:** ghim phiên bản Python của môi trường test cho giống image chạy thật.

## 2026-11-12 – Lợi suất công bố trễ một ngày

- **Vấn đề:** DAG chạy lúc 01:00 UTC cho ngày D, nhưng FRED thường công bố lợi suất của D
  vào chiều D+1 (giờ Mỹ). Nếu chỉ xử lý `dt = D` thì lợi suất của D không bao giờ vào silver.
- **Giải pháp:** mỗi lần chạy đọc lại 7 ngày `rates.yields` gần nhất
  (`YIELD_LATENCY_DAYS`), ghi lại các phân vùng `yield_curve` tương ứng; cổng chất lượng
  chỉ cảnh báo khi lợi suất của D còn trống.
- **Bài học:** mỗi nguồn có độ trễ công bố riêng; lịch batch phải tính theo nguồn chậm nhất.
