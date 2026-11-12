# Speed layer (role B)

```text
Kafka topics
  │  structured streaming, watermark 2m
  ▼
1-minute OHLCV bars
  │  foreachBatch -> shared pandas indicators (SMA/EMA/MACD/RSI/z-score)
  ▼
bars + alerts ──► Cassandra (ohlcv_1m, alerts)
      └─────────► market.alerts Kafka topic
```

## Run

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  streaming/job.py \
  --bootstrap localhost:9092 \
  --checkpoint s3a://checkpoints/speed \
  --cassandra-host cassandra
```

For a local filesystem checkpoint and Parquet output (no Cassandra):

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  streaming/job.py --checkpoint /tmp/checkpoints/speed \
  --output-mode parquet --parquet-path /tmp/speed/bars
```

## Guarantees and caveats

- Kafka source + checkpoint gives at-least-once processing.
- Cassandra writes are upserts by primary key, so normal replay is effectively
  exactly-once for the sink. Alert Kafka writes can duplicate on replay by design.
- Indicators are computed per micro-batch with shared pandas logic. This is enough for
  the BTL demo, but a production job should keep rolling state across batches (for
  example `applyInPandasWithState` or a wider lookback loaded from Cassandra).
- Late events are handled by the 2-minute watermark; events older than the watermark
  are dropped by Spark.
- `failOnDataLoss=false` prevents a stopped producer from killing the query.

## Cassandra tables

The contract is in `contracts/cassandra/schema.cql`. Role B implements the sink, role A
owns the schema. Run the CQL once before starting the job.

## Tests

```bash
pytest streaming/tests
```

The pure batch logic is tested without Spark; Spark integration needs the packages above.

## Đo độ trễ và chứng minh exactly-once (việc B.5, B.6)

Metrics Prometheus trên driver, cổng `9108` (`/metrics`): `market_speed_input_rows_per_second`,
`market_speed_processed_rows_per_second`, `market_speed_batch_duration_ms`,
`market_speed_offsets_behind_latest`, `market_speed_event_to_sink_latency_seconds` (nhãn `query`).

Đo độ trễ giao dịch → Cassandra và ghi ngay vào nhật ký số liệu:

```bash
python -m streaming.tools.measure_latency --day 2026-11-20 \
  --cassandra-host market-dc1-service.cassandra.svc \
  --note "trigger 5s, 1 executor" --append-metrics docs/metrics/B-speed.md
```

Quy trình chứng minh exactly-once trên cụm:

1. Chạy producer replay một tập file cố định (ví dụ 1 giờ dữ liệu của bronze), đồng thời giữ
   bản sao JSONL của tập đó (`bronze-dump/*.json`).
2. Giữa chừng xoá pod driver:
   `kubectl -n spark delete pod -l spark-role=driver,sparkoperator.k8s.io/app-name=speed-layer`.
   Spark Operator (restartPolicy Always) tạo lại driver, query đọc lại checkpoint
   `s3a://checkpoints/speed-ohlcv-1m/` và chạy lại batch đang dở.
3. Khi replay xong, so nến trong Cassandra với nến tính lại từ dữ liệu gốc:
   `python -m streaming.tools.exactly_once_check --trades 'bronze-dump/*.json' --cassandra-host market-dc1-service.cassandra.svc`
   Exit code 0 nghĩa là không thiếu nến, không có nến lạ, `trade_count`/`volume` không bị cộng hai lần.
4. Ghi kết quả (số lần xoá pod, thời gian phục hồi, kết quả check) vào `docs/metrics/B-speed.md`.

Kịch bản này đã được tự động hoá ở mức local trong
`streaming/tests/test_tools.py::test_replayed_micro_batch_is_not_double_counted`: test xoá commit
cuối của checkpoint (đúng trạng thái một driver chết sau khi ghi sink nhưng trước khi commit),
khởi động lại, rồi chạy `exactly_once_check`.
