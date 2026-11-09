"""The DAG module must import even when Airflow is absent (local CI)."""

from __future__ import annotations

import importlib


def test_dag_module_imports_without_airflow() -> None:
    module = importlib.import_module("batch.dags.market_lakehouse_dag")
    assert module.PROJECT_DIR
    assert module.MINIO_ENDPOINT.startswith("http")
