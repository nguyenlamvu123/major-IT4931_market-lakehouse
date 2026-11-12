# Chương C – Batch layer & Data Lake

> Mẫu chương theo PDF "Kiến trúc – Phân công": mỗi người viết chương của mình gồm 4 phần
> (vấn đề, giải pháp, số liệu, bài học). Chỗ `…` điền sau khi đo trên cụm (tuần 4–5).

## 1. Vấn đề

- Luồng realtime (B) chỉ thấy vài phút gần nhất và chấp nhận sai số: dữ liệu đến muộn quá
  watermark bị bỏ, và chỉ báo tính trên cửa sổ ngắn.
- Các câu hỏi phân tích trong PDF cần lịch sử nhiều tháng và **join nhiều nguồn khác lịch**:
  cổ phiếu (ngày giao dịch NYSE), trái phiếu (lịch SIFMA, nghỉ cả Columbus/Veterans Day),
  vàng/DXY (Yahoo), crypto (24/7).
- Dữ liệu thô có lỗi: trùng khi producer kết nối lại, giá âm, high < low, JSON hỏng, FRED
  công bố trễ một ngày và có sửa số.

## 2. Giải pháp

- Mô hình bronze → silver → gold trên MinIO (Parquet, phân vùng `dt` UTC), chạy lại một
  ngày là idempotent nhờ dynamic partition overwrite.
- Silver: luật hợp đồng cho từng dòng (`reject_reason`), bỏ trùng tất định theo
  `trade_id` / `bar_id` / `alert_id` / `(series_id, day)`, thống kê dòng bị loại.
- Cổng chất lượng giữa silver và gold: lỗi thì dừng DAG trước khi gold bị ghi đè.
- Gold bằng Spark:
  - daily theo thứ tự ưu tiên nguồn (nến ngày yfinance > trades > nến phút);
  - lịch chuẩn là các ngày SPY giao dịch, forward-fill lợi suất tối đa 3 ngày;
  - tương quan 90 ngày giữa lợi suất của giá và thay đổi của lợi suất trái phiếu;
  - đường cong lợi suất có spread và cờ đảo ngược.
- MLlib KMeans phân cụm 12 tài sản theo lợi suất, độ biến động và tương quan với 10Y.
- Ghi Cassandra theo đúng `contracts/cassandra.cql` (kiểm tra cột/kiểu bằng test).
- Airflow DAG lúc 01:00 UTC; mỗi bước là một SparkApplication trên Spark Operator.

Kỹ thuật Spark đã dùng (6 nhóm kỹ năng của môn, phần của C):

| Nhóm | Ở đâu |
|---|---|
| Join | lịch chuẩn × chuỗi giá/lợi suất, self-join theo ngày để tính tương quan, join correlation vào features |
| Transformation | `reject_reason` theo luật, `timestamp_millis`, pivot kỳ hạn lợi suất |
| Aggregation / window | `min_by`/`max_by` cho open/close, `lag`, `stddev_samp` trên window 20 dòng, `last(ignorenulls)` để forward-fill, `row_number` để bỏ trùng |
| Hiệu năng | partition pruning theo `dt`, AQE, số shuffle partition, broadcast join, cache |
| MLlib | VectorAssembler + StandardScaler + KMeans + ClusteringEvaluator |

## 3. Số liệu

| Số liệu | Giá trị | Nguồn |
|---|---|---|
| Spark gold so với bản tham chiếu pandas | khớp trong sai số 1e-6 | `batch/tests/test_gold.py` |
| Partition pruning | ×2,4 (local) / … (cụm) | `docs/metrics/C-batch.md` |
| 200 → 8 shuffle partition | ×5,6 (local) / … | như trên |
| AQE với 200 partition | ×2,1 (local) / … | như trên |
| Cache | ×1,8 (local) / … | như trên |
| Broadcast join | ×1,04 (local), SortMergeJoin 7 → 3 / … | như trên |
| Silhouette KMeans | 0,60 (dữ liệu giả lập) / … | log của `train_kmeans` |
| % dòng bronze bị chặn | … | `silver/_stats` |
| DAG chạy thành công 3 đêm liên tiếp | … | Airflow UI |

## 4. Bài học

Xem `docs/lessons/C-batch.md`. Tóm tắt:

- Phải chốt múi giờ của session Spark (UTC); `dt` sai một ngày là hỏng cả lịch.
- Cấu trúc thư mục bronze quyết định cách đọc. Spark không đọc được nhiều
  `<topic>/dt=` trong một lần đọc.
- Mỗi phép tính Spark cần một bản tham chiếu đơn giản để so sánh; cách này bắt được lỗi
  forward-fill và cửa sổ tương quan ngay khi viết.
