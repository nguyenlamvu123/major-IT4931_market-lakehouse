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
