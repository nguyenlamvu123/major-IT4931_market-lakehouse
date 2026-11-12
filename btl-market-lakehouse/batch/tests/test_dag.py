"""The nightly DAG and its SparkApplication templates (no Airflow / cluster needed)."""

from __future__ import annotations

import importlib
import re
from pathlib import Path

import yaml

BATCH = Path(__file__).resolve().parents[1]
RENDER = {"{{ ds }}": "2026-11-09", "{{ ds_nodash }}": "20261109"}


def _render(template: str) -> str:
    for placeholder, value in RENDER.items():
        template = template.replace(placeholder, value)
    return template


def _load(name: str) -> dict:
    return yaml.safe_load(_render((BATCH / "k8s" / name).read_text(encoding="utf-8")))


def test_dag_module_imports_without_airflow_and_declares_order() -> None:
    module = importlib.import_module("batch.dags.market_lakehouse_dag")
    assert module.SCHEDULE == "0 1 * * *"
    assert [task for task, _ in module.PIPELINE] == [
        "bronze_to_silver",
        "quality_checks",
        "silver_to_gold",
        "cassandra_sink",
        "train_kmeans",
    ]
    assert module.K8S_DIR == BATCH / "k8s"
    for _, template in module.PIPELINE:
        assert (module.K8S_DIR / template).exists(), template


def test_templates_only_use_airflow_ds_macros() -> None:
    for path in (BATCH / "k8s").glob("*.yaml"):
        text = path.read_text(encoding="utf-8")
        assert "{{ ds }}" in text, path.name
        assert re.findall(r"{{.*?}}", _render(text)) == [], path.name


def test_templates_follow_the_infrastructure_contract() -> None:
    module = importlib.import_module("batch.dags.market_lakehouse_dag")
    for task_id, template in module.PIPELINE:
        document = _load(template)
        spec = document["spec"]
        assert document["kind"] == "SparkApplication"
        assert document["metadata"]["namespace"] == "spark"
        assert document["metadata"]["name"].endswith("-20261109")
        assert spec["image"] == "btl/batch:dev"
        assert spec["imagePullPolicy"] == "IfNotPresent"
        assert spec["driver"]["serviceAccount"] == "spark-operator-spark"

        main_file = spec["mainApplicationFile"].removeprefix("local:///opt/app/")
        assert (BATCH.parent / main_file).exists(), main_file
        assert Path(main_file).stem == task_id
        arguments = spec["arguments"]
        assert arguments[arguments.index("--dt") + 1] == "2026-11-09"
        assert arguments[arguments.index("--lake") + 1] == "s3a://lake"

        env = {item["name"]: item for item in spec["driver"]["env"]}
        assert env["MINIO_ENDPOINT"]["value"] == "http://minio.minio.svc:9000"
        assert env["MINIO_SECRET_KEY"]["valueFrom"]["secretKeyRef"]["name"] == "minio-credentials"
        if task_id in {"cassandra_sink", "train_kmeans"}:
            assert env["CASSANDRA_HOSTS"]["value"] == "market-dc1-service.cassandra.svc"


def test_quality_gate_is_not_retried_by_the_operator() -> None:
    # A failed rule is deterministic; Spark Operator retries would only delay the alert.
    assert _load("quality_checks.yaml")["spec"]["restartPolicy"]["type"] == "Never"
    assert _load("silver_to_gold.yaml")["spec"]["restartPolicy"]["type"] == "OnFailure"
