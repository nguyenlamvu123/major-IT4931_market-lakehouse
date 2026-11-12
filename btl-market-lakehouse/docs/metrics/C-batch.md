# Số liệu đo – vai C (Batch layer)

Theo PDF: "Số liệu cho báo cáo: ghi ngay khi đo vào docs/metrics (ngày, cấu hình, kết quả).
Cuối kỳ không phải đo lại." Việc C.6 yêu cầu "Tối ưu hiệu năng có số liệu trước/sau: số
partition, broadcast join, cache, AQE".

## 1. Tối ưu silver → gold (đo local, 2026-11-12)

> **Đây là số đo trên laptop ở chế độ `local[2]`**, đọc/ghi file local, không có mạng tới
> MinIO và chỉ có một JVM. Nó cho biết *chiều* và *độ lớn tương đối* của từng kỹ thuật.
> Tuần 5 phải đo lại trên cụm (Spark Operator, 2 executor, MinIO) bằng đúng lệnh bên dưới
> và ghi thêm một bảng "cụm" vào file này.

Lệnh tái lập (sinh dữ liệu silver giả lập bằng Spark rồi đo):

```bash
python batch/jobs/benchmark.py --lake file:///tmp/bench --days 365 --rows-per-day 2000 \
  --repeat 1 --master 'local[2]' --report /tmp/bench-table.md
```

- Ngày đo: 2026-11-12
- Máy: Linux x86_64, 16 CPU, Python 3.12.3, Spark 3.5.3, master `local[2]`, driver 1 GB
- Dữ liệu: 556,736 dòng silver trades giả lập trong 365 ngày (12 mã), tính gold cho dt=2026-11-25 với lookback 150 ngày
- 1 lần khởi động chung, mỗi biến thể lấy trung vị của 1 lần đo, ghi vào sink `noop`

| Kỹ thuật | Trước | Giây | Sau | Giây | Tăng tốc | Ghi chú (plan) |
|---|---|---:|---|---:|---:|---|
| Partition pruning theo dt | lọc to_date(event_time) (quét mọi dt) | 0.72 | lọc cột phân vùng dt | 0.29 | x2.44 | PartitionFilters: [dt >= …, dt <= …] |
| Số shuffle partition (AQE tắt) | 200 (mặc định) | 30.72 | 8 | 5.49 | x5.60 | 200 task gần rỗng mỗi stage trên local[2] |
| AQE (200 partition) | tắt | 30.72 | bật | 14.67 | x2.09 | AQE gộp partition nhỏ sau shuffle (coalescePartitions) |
| Broadcast join bảng nhỏ (AQE tắt, 8 partition) | tắt (autoBroadcastJoinThreshold=-1) | 5.71 | bật (10 MB) | 5.49 | x1.04 | SortMergeJoin 7 → 3, BroadcastHashJoin 0 → 4 |
| Cache DataFrame dùng lại | không cache | 10.72 | cache daily/changes/corr | 5.87 | x1.83 | daily_from_trades chạy 1 lần thay vì mỗi bảng gold 1 lần |

Tổng thời gian chạy cả benchmark: 1 phút 49 giây.

### Đọc kết quả

- **Partition pruning (x2.4).** Gold cần 150 ngày lịch sử trong 365 ngày silver. Lọc trên
  cột phân vùng `dt` thì Spark chỉ liệt kê và đọc 151 thư mục; lọc trên
  `to_date(event_time)` thì phải mở mọi file rồi mới lọc. `batch.silver.read_silver`
  luôn lọc theo `dt`, và plan có dòng `PartitionFilters`.
- **Số shuffle partition (x5.6).** Mặc định 200 partition sinh ra 200 task gần rỗng cho
  mỗi stage có shuffle, trong khi máy chỉ có 2 core. `build_spark` và các
  SparkApplication đặt `spark.sql.shuffle.partitions=8`. Trên cụm, nên đặt khoảng 2–3 lần
  tổng số core của executor.
- **AQE (x2.1 khi giữ 200 partition).** AQE gộp các partition nhỏ sau shuffle, nên lấy lại
  phần lớn thời gian bị mất ở trên mà không phải chỉnh tay. Vẫn bật AQE ở cấu hình chạy
  thật, vì với dữ liệu thật nó còn tách skew (AAPL, NVDA, BTC có nhiều giao dịch hơn hẳn).
- **Broadcast join (x1.04).** Bảng lịch, bảng ngày yêu cầu và lợi suất rất nhỏ. Bật
  broadcast thì 4 phép join chuyển từ SortMergeJoin sang BroadcastHashJoin (SortMergeJoin
  7 → 3). Ở local gần như không đổi vì shuffle không qua mạng. Trên cụm, mỗi SortMergeJoin
  bỏ được là bỏ được một lần shuffle qua mạng.
- **Cache (x1.8).** `daily`, các thay đổi đã căn lịch và ma trận tương quan cùng nuôi
  nhiều bảng gold. Không cache thì phép tổng hợp trades chạy lại cho từng bảng.
  `build_gold(cache=True)` là mặc định.

## 2. Kiểm thử tương đương với bản tham chiếu pandas

`batch/tests/test_gold.py` chạy Spark và `common.analytics` trên cùng thị trường giả lập
90 ngày. `daily_summary`, `correlation_matrix` (kể cả US2Y/US10Y/REAL10Y) và
`yield_curve` khớp nhau trong sai số 1e-6.

## 3. Còn phải đo trên cụm (tuần 4–5)

| Số liệu | Cách đo | Ghi vào |
|---|---|---|
| Thời gian từng task của DAG | Airflow UI → Task duration, 3 đêm liên tiếp | bảng mới ở mục 1 |
| % bản ghi bị chặn ở silver | `silver/_stats/dt=…` (rejected_rows / input_rows) | bảng chất lượng |
| Bảng benchmark trên cụm | `benchmark.py` trong một SparkApplication, `--lake s3a://bench` | bảng "cụm" |
