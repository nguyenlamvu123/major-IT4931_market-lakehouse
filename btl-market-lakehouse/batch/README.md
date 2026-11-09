# Batch layer (role C)

```text
s3a://lake/bronze/*.json        (written by Kafka Connect, role A)
        │  bronze_to_silver.py
        ▼
s3a://lake/silver/trades        Parquet, deduped by trade_id, partitioned by asset_class/dt
        │  silver_to_gold.py
        ▼
s3a://lake/gold/
├── daily_bars/
├── correlation_matrix/
├── yield_curve/                (when rates path is supplied)
└── ml_features/
```

## Run with Spark

```bash
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  batch/jobs/bronze_to_silver.py \
  --bronze s3a://lake/bronze --silver s3a://lake/silver/trades \
  --minio-endpoint http://minio:9000 --access-key minioadmin --secret-key minioadmin

spark-submit batch/jobs/silver_to_gold.py \
  --silver s3a://lake/silver/trades --gold s3a://lake/gold \
  --rates s3a://lake/bronze/rates.ticks \
  --minio-endpoint http://minio:9000 --access-key minioadmin --secret-key minioadmin
```

## Run locally without Spark

```bash
python batch/run_local_batch.py
```

Outputs go to `data/batch/silver/` and `data/batch/gold/`.

## Tests

```bash
pytest batch/tests
```

## Design notes

- Bronze is the immutable source; silver is the deduplicated, typed Parquet table.
- `trade_id` is the deduplication key. The same file can be reprocessed safely.
- Gold is rebuilt with `overwrite` for the BTL demo. In production, use partition
  overwrite by date and keep a manifest/run id.
- Correlation and ML feature matrices are small enough to compute on the driver for the
  BTL; the Spark jobs handle the heavy silver data.
- MLlib clustering lives in `feature/C-mllib`; the Airflow DAG lives in
  `feature/C-airflow`.
