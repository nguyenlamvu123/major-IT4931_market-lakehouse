"""Static checks of the Docker Compose stacks (no Docker needed)."""

from __future__ import annotations

from pathlib import Path

import yaml

from common.contracts import TOPICS
from infra.scripts.topics_contract import load_topics

INFRA = Path(__file__).resolve().parents[1]


def _compose(name: str) -> dict:
    return yaml.safe_load((INFRA / name).read_text(encoding="utf-8"))


def _all_services() -> dict[str, dict]:
    services = dict(_compose("docker-compose.dev.yml")["services"])
    services.update(_compose("docker-compose.full.yml")["services"])
    return services


def test_topic_loader_reads_all_contract_topics() -> None:
    topics = load_topics()
    assert len(topics) == 8
    assert {topic.name for topic in topics} == set(TOPICS)
    for topic in topics:
        assert topic.partitions == TOPICS[topic.name]["partitions"]
        assert topic.retention_ms == TOPICS[topic.name]["retention_ms"]


def test_init_topics_script_has_no_hard_coded_topics() -> None:
    script = (INFRA / "scripts" / "init-topics.sh").read_text(encoding="utf-8")
    assert "topics_contract" in script
    for name in TOPICS:
        assert name not in script


def test_no_bitnami_images() -> None:
    # Regression: bitnami/kafka and bitnami/spark left the free catalog in 2025.
    for name, service in _all_services().items():
        assert "bitnami" not in service.get("image", ""), name


def test_kafka_has_host_and_internal_listeners() -> None:
    kafka = _compose("docker-compose.dev.yml")["services"]["kafka"]
    assert kafka["image"].startswith("apache/kafka:3.7")
    advertised = kafka["environment"]["KAFKA_ADVERTISED_LISTENERS"]
    assert "localhost:9092" in advertised
    assert "kafka:29092" in advertised
    assert kafka["environment"]["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] == "false"


def test_init_jobs_use_contract_files() -> None:
    dev = _compose("docker-compose.dev.yml")["services"]
    cassandra_init = dev["cassandra-init"]
    assert "../contracts:/contracts:ro" in cassandra_init["volumes"]
    assert "/contracts/cassandra.cql" in cassandra_init["entrypoint"]
    minio_init = "\n".join(dev["minio-init"]["command"])
    assert "local/lake" in minio_init
    assert "local/checkpoints" in minio_init


def test_full_stack_includes_dev_and_uses_contract_images() -> None:
    full = _compose("docker-compose.full.yml")
    assert full["include"] == ["docker-compose.dev.yml"]
    services = full["services"]
    assert services["kafka-connect"]["image"] == "btl/kafka-connect-s3:dev"
    assert services["kafka-connect"]["environment"]["CONNECT_BOOTSTRAP_SERVERS"] == "kafka:29092"
    assert services["market-api"]["image"] == "btl/market-api:dev"
    assert services["airflow"]["image"] == "btl/airflow:dev"
    assert services["spark-master"]["image"].startswith("apache/spark:3.5")
    # No service may be declared in both files (include would reject the duplicate).
    dev_services = set(_compose("docker-compose.dev.yml")["services"])
    assert dev_services.isdisjoint(services)


def test_no_copied_cql_or_topic_lists_in_infra() -> None:
    for path in INFRA.rglob("*"):
        if path.is_dir() or path.suffix not in {".yml", ".yaml", ".sh", ".cql"}:
            continue
        # The legacy plain manifests in infra/k8s are replaced by operator CRs in the next
        # change (feature/D-k8s-operators), which also drops this exclusion.
        if "k8s" in path.relative_to(INFRA).parts:
            continue
        text = path.read_text(encoding="utf-8")
        assert "CREATE TABLE" not in text, path
        assert "equity.trades" not in text, path
        assert "market.alerts" not in text, path
