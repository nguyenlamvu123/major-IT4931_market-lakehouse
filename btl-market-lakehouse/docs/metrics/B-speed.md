# Số liệu Speed layer (vai B)

Ghi ngay khi đo (PDF, "Quy ước nhóm": ngày, cấu hình, kết quả). `measure_latency
--append-metrics docs/metrics/B-speed.md` tự thêm dòng vào bảng dưới.

- **Độ trễ** = `updated_at − last_event_time` của từng lần upsert: thời gian từ giao dịch mới
  nhất của nến tới lúc nến được ghi.
- **Độ trễ xấu nhất cho một giao dịch** ≈ trigger + thời gian batch dài nhất (giao dịch đến
  ngay sau một trigger phải chờ trigger kế tiếp).

| Thời điểm | Phép đo | Cấu hình | Kết quả |
|---|---|---|---|
| 2026-11-12 11:20 UTC | latency trade->ohlcv_1m (local, nguồn file) | `local_latency_run --seconds 60`; local[2], driver 1g, trigger 5s, watermark 30s, 200 trade/s (crypto), sink Parquet | n=26 p50=0.48s p95=2.10s max=2.52s |
| 2026-11-12 11:20 UTC | latency trade->ohlcv_5m (local, nguồn file) | như trên | n=28 p50=0.76s p95=2.42s max=2.73s |
| 2026-11-12 11:20 UTC | tốc độ Spark (local) | như trên, 13–14 micro-batch có dữ liệu | 1m: 937 dòng/s xử lý trung bình, batch dài nhất 4.30s; 5m: 1041 dòng/s, batch dài nhất 4.36s |
| 2026-11-12 11:20 UTC | độ trễ xấu nhất ước tính (local) | trigger 5s + batch dài nhất | ≈ 9.3s (< mục tiêu ~10s của PDF) |
| 2026-11-12 | exactly-once (local) | `test_replayed_micro_batch_is_not_double_counted`: xoá commit cuối, chạy lại | batch được chạy lại (Parquet có thêm phiên bản), `exactly_once_check` ok: 0 nến thiếu/thừa, 0 lệch trade_count/volume |

Cần đo trên cụm (tuần 4–5): độ trễ với nguồn Kafka + sink Cassandra, tốc độ khi replay 10x/100x,
kết quả xoá pod driver (thời gian phục hồi, `exactly_once_check`).
