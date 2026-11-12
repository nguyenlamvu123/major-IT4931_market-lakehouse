"""The SparkApplication must use the names fixed in contracts/infra.yaml."""

from __future__ import annotations

from pathlib import Path

import yaml

from streaming.job import BAR_QUERIES, parse_args

ROOT = Path(__file__).resolve().parents[2]


def _documents() -> dict[str, dict]:
    path = ROOT / "streaming" / "k8s" / "speed-layer.yaml"
    return {doc["kind"]: doc for doc in yaml.safe_load_all(path.read_text(encoding="utf-8"))}


def test_spark_application_follows_infra_contract() -> None:
    infra = yaml.safe_load((ROOT / "contracts" / "infra.yaml").read_text(encoding="utf-8"))
    app = _documents()["SparkApplication"]
    spec = app["spec"]
    assert app["metadata"]["namespace"] == "spark"
    assert spec["image"] in infra["images"]
    assert spec["image"] == "btl/streaming:dev"
    assert spec["imagePullPolicy"] == "IfNotPresent"
    assert spec["restartPolicy"]["type"] == "Always"
    assert spec["driver"]["serviceAccount"] == infra["namespaces"]["spark"]["service_account"]
    assert spec["driver"]["ports"] == [
        {
            "name": "metrics",
            "containerPort": infra["metrics"]["streaming_driver"]["port"],
            "protocol": "TCP",
        }
    ]

    # The arguments must parse with the real CLI and point at the contract services.
    args = parse_args(spec["arguments"])
    assert args.source == "kafka"
    assert args.bootstrap == infra["services"]["kafka_bootstrap"]
    assert f"{args.cassandra_host}:9042" == infra["services"]["cassandra"]
    assert args.checkpoint_root == "s3a://checkpoints"
    assert args.watermark == "30 seconds"
    assert args.trigger == "5 seconds"
    assert args.metrics_port == 9108

    env = {item["name"]: item for item in spec["driver"]["env"]}
    assert env["MINIO_ENDPOINT"]["value"] == infra["services"]["minio"]
    for key in ("MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"):
        ref = env[key]["valueFrom"]["secretKeyRef"]
        assert ref["name"] == "minio-credentials"
        assert key in infra["secrets"]["minio-credentials"]


def test_checkpoints_follow_paths_contract() -> None:
    paths = yaml.safe_load((ROOT / "contracts" / "paths.yaml").read_text(encoding="utf-8"))
    assert [query.name for query in BAR_QUERIES] == paths["checkpoints"]["jobs"]


def test_prometheus_scrapes_driver_metrics_port() -> None:
    docs = _documents()
    monitor = docs["PodMonitor"]["spec"]
    assert monitor["selector"]["matchLabels"] == docs["Service"]["spec"]["selector"]
    assert {"port": "metrics", "path": "/metrics", "interval": "15s"} in monitor[
        "podMetricsEndpoints"
    ]
