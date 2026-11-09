"""Daily batch DAG. Imports safely even when Airflow is not installed."""

from __future__ import annotations

import os
from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
except ImportError:  # local pytest / no Airflow runtime
    DAG = None  # type: ignore[assignment]

PROJECT_DIR = os.getenv("MARKET_PROJECT_DIR", "/opt/app")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4"

if DAG is not None:
    default_args = {
        "owner": "btl-market-lakehouse",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "depends_on_past": False,
    }

    with DAG(
        dag_id="market_lakehouse_batch",
        description="Bronze -> silver -> gold -> MLlib clusters",
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        default_args=default_args,
        tags=["btl", "market", "batch"],
    ) as dag:
        bronze_to_silver = SparkSubmitOperator(
            task_id="bronze_to_silver",
            conn_id="spark_default",
            application=f"{PROJECT_DIR}/batch/jobs/bronze_to_silver.py",
            application_args=[
                "--bronze",
                "s3a://lake/bronze",
                "--silver",
                "s3a://lake/silver/trades",
                "--minio-endpoint",
                MINIO_ENDPOINT,
                "--access-key",
                MINIO_ACCESS_KEY,
                "--secret-key",
                MINIO_SECRET_KEY,
            ],
            packages=SPARK_PACKAGES,
            verbose=False,
        )

        silver_to_gold = SparkSubmitOperator(
            task_id="silver_to_gold",
            conn_id="spark_default",
            application=f"{PROJECT_DIR}/batch/jobs/silver_to_gold.py",
            application_args=[
                "--silver",
                "s3a://lake/silver/trades",
                "--gold",
                "s3a://lake/gold",
                "--rates",
                "s3a://lake/bronze/rates.ticks",
                "--minio-endpoint",
                MINIO_ENDPOINT,
                "--access-key",
                MINIO_ACCESS_KEY,
                "--secret-key",
                MINIO_SECRET_KEY,
            ],
            packages=SPARK_PACKAGES,
            verbose=False,
        )

        train_clusters = SparkSubmitOperator(
            task_id="train_asset_clusters",
            conn_id="spark_default",
            application=f"{PROJECT_DIR}/batch/jobs/train_kmeans.py",
            application_args=[
                "--gold",
                "s3a://lake/gold",
                "--k",
                "3",
                "--evaluate",
                "--minio-endpoint",
                MINIO_ENDPOINT,
                "--access-key",
                MINIO_ACCESS_KEY,
                "--secret-key",
                MINIO_SECRET_KEY,
            ],
            packages=SPARK_PACKAGES,
            verbose=False,
        )

        data_quality = BashOperator(
            task_id="data_quality_checks",
            bash_command=f"cd {PROJECT_DIR} && python -m pytest batch/tests -q",
        )

        bronze_to_silver >> silver_to_gold >> train_clusters >> data_quality
