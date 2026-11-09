# Airflow (role C)

The DAG `market_lakehouse_batch` runs daily:

```text
bronze_to_silver -> silver_to_gold -> train_asset_clusters -> data_quality_checks
```

## Requirements

- Airflow 2.8+ with `apache-airflow-providers-apache-spark`
- A `spark_default` connection pointing at the Spark master
- The project mounted at `/opt/app` (or set `MARKET_PROJECT_DIR`)
- MinIO/S3A credentials available to the Spark submit environment

## Environment

```bash
export MARKET_PROJECT_DIR=/opt/app
export MINIO_ENDPOINT=http://minio:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
```

## Deploy

```bash
cp batch/dags/market_lakehouse_dag.py "$AIRFLOW_HOME/dags/"
airflow dags list | grep market_lakehouse
airflow dags trigger market_lakehouse_batch
```

## Why this is separate from the local demo

Airflow, Spark and MinIO need more RAM than the current machine has. The DAG is
provided as production code and is import-safe without Airflow, so the repo can still
run `pytest` locally. Use `python batch/run_local_batch.py` for the local render.

## Notes

- The DAG uses partition overwrite only in the silver/gold jobs; for production, add a
  run-id partition and a manifest table.
- `data_quality_checks` currently runs the batch unit tests. Replace it with Great
  Expectations or dbt tests if the team wants stronger contracts.
