"""Nightly batch DAG of the Market Lakehouse (PDF task C.7).

Call stack (one SparkApplication per task, submitted to the Spark Operator):

market_lakehouse_batch  (01:00 UTC daily, processes dt = {{ ds }})
  -> bronze_to_silver   batch/k8s/bronze_to_silver.yaml -> batch/jobs/bronze_to_silver.py
  -> quality_checks     batch/k8s/quality_checks.yaml   -> batch/jobs/quality_checks.py
  -> silver_to_gold     batch/k8s/silver_to_gold.yaml   -> batch/jobs/silver_to_gold.py
  -> cassandra_sink     batch/k8s/cassandra_sink.yaml   -> batch/jobs/cassandra_sink.py
  -> train_kmeans       batch/k8s/train_kmeans.yaml     -> batch/jobs/train_kmeans.py

Schedule: 01:00 UTC is 08:00 in Vietnam, four hours after the US close (04:00 VN). The
run for ``ds`` = D starts at D+1 01:00 UTC, when the whole UTC day D is in bronze.
FRED yields of D are often published later; the jobs re-read a trailing week of
rates.yields (``batch.lake.YIELD_LATENCY_DAYS``).

The module is self-contained (no ``batch`` imports) so the Airflow image only needs this
file plus ``batch/k8s/``, and it imports without Airflow for local tests.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

DAG_ID = "market_lakehouse_batch"
SCHEDULE = "0 1 * * *"

#: Task id -> SparkApplication template, in dependency order. Kept as data so tests can
#: check the order without Airflow installed.
PIPELINE: tuple[tuple[str, str], ...] = (
    ("bronze_to_silver", "bronze_to_silver.yaml"),
    ("quality_checks", "quality_checks.yaml"),
    ("silver_to_gold", "silver_to_gold.yaml"),
    ("cassandra_sink", "cassandra_sink.yaml"),
    ("train_kmeans", "train_kmeans.yaml"),
)

#: Where the SparkApplication templates live inside the Airflow image. Defaults to the
#: repo layout (batch/dags/../k8s); the image sets MARKET_BATCH_K8S_DIR explicitly.
K8S_DIR = Path(os.getenv("MARKET_BATCH_K8S_DIR", str(Path(__file__).resolve().parents[1] / "k8s")))
SPARK_NAMESPACE = os.getenv("SPARK_NAMESPACE", "spark")
KUBERNETES_CONN_ID = os.getenv("SPARK_KUBERNETES_CONN_ID", "kubernetes_default")

#: Week 1 of the project (PDF plan starts 09/11/2026). With catchup=True Airflow runs every
#: missed day since then, one at a time (max_active_runs=1), which bounds the backfill.
START_DATE = datetime(2026, 11, 9, tzinfo=timezone.utc)

DEFAULT_ARGS = {
    "owner": "C-batch",
    "depends_on_past": False,
    # Infrastructure hiccups (pod eviction, MinIO restart) are retried; a failing quality
    # rule fails again on retry, which is acceptable noise for two retries.
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

try:
    from airflow import DAG
    from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
        SparkKubernetesOperator,
    )
except ImportError:  # local pytest / CI without Airflow
    DAG = None  # type: ignore[assignment,misc]

if DAG is not None:
    with DAG(
        dag_id=DAG_ID,
        description="bronze -> silver -> quality -> gold -> Cassandra -> KMeans",
        schedule=SCHEDULE,
        start_date=START_DATE,
        catchup=True,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        template_searchpath=[str(K8S_DIR)],
        tags=["btl", "market", "batch", "C"],
    ) as dag:
        previous = None
        for task_id, template in PIPELINE:
            task = SparkKubernetesOperator(
                task_id=task_id,
                namespace=SPARK_NAMESPACE,
                application_file=template,
                kubernetes_conn_id=KUBERNETES_CONN_ID,
                # The template already makes the name unique per day; the random suffix
                # also separates Airflow retries of the same day.
                random_name_suffix=True,
                delete_on_termination=True,
                get_logs=True,
            )
            if previous is not None:
                previous >> task
            previous = task
