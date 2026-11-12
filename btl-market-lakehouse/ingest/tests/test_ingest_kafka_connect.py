"""Kafka Connect S3 sink configs against contract v2 (topics, bronze path, partitioner)."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from common.contracts import TOPICS

PROJECT = Path(__file__).resolve().parents[2]
CONNECT = PROJECT / "ingest" / "kafka_connect"
K8S = PROJECT / "ingest" / "k8s" / "kafka-connect"


def _json(name: str) -> dict:
    return json.loads((CONNECT / name).read_text(encoding="utf-8"))


def _topics(config: dict) -> set[str]:
    return set(config["topics"].split(","))


def test_sinks_cover_every_contract_topic_except_dead_letter() -> None:
    main = _json("s3-sink.json")["config"]
    alerts = _json("s3-sink-alerts.json")["config"]
    assert _topics(main) | _topics(alerts) == set(TOPICS) - {"dead-letter"}
    assert _topics(main) & _topics(alerts) == set()
    assert _topics(alerts) == {"alerts"}


def test_event_time_partitioning_only_on_topics_that_have_event_time() -> None:
    main = _json("s3-sink.json")["config"]
    assert main["timestamp.extractor"] == "RecordField"
    assert main["timestamp.field"] == "event_time"
    for topic in _topics(main):
        schema_file = {"trade": "trade", "bar": "bar", "rate_yield": "rate_yield"}[
            TOPICS[topic]["schema"]
        ]
        schema = json.loads((PROJECT / "contracts" / "schemas" / f"{schema_file}.schema.json")
                            .read_text())
        assert "event_time" in schema["required"], topic
    assert _json("s3-sink-alerts.json")["config"]["timestamp.extractor"] == "Record"


# ROOT CAUSE:
#
# The v1 sink used TimeBasedPartitioner without partition.duration.ms, locale and
# timezone (all required by the partitioner), had no storage.class, and set
# timestamp.extractor=Record, which ignores timestamp.field=event_time.
#
# We fixed this by declaring every required key, RecordField extraction and a daily
# 'dt'=YYYY-MM-dd path matching contracts/paths.yaml.
def test_time_based_partitioner_has_all_required_keys() -> None:
    for name in ("s3-sink.json", "s3-sink-alerts.json"):
        config = _json(name)["config"]
        assert config["partitioner.class"].endswith("TimeBasedPartitioner")
        for key in ("partition.duration.ms", "path.format", "locale", "timezone",
                    "storage.class", "format.class", "s3.bucket.name", "flush.size"):
            assert config.get(key), (name, key)
        assert config["partition.duration.ms"] == "86400000"
        assert config["timezone"] == "UTC"
        assert config["topics.dir"] == "bronze"
        assert config["path.format"] == "'dt'=YYYY-MM-dd"
        assert config["s3.bucket.name"] == "lake"
        assert config["errors.deadletterqueue.topic.name"] == "dead-letter"


def test_bronze_layout_matches_paths_contract() -> None:
    paths = yaml.safe_load((PROJECT / "contracts" / "paths.yaml").read_text())
    assert paths["bronze"]["pattern"] == "s3a://lake/bronze/{topic}/dt={YYYY-MM-DD}/"
    assert set(paths["bronze"]["topics"]) == set(TOPICS) - {"dead-letter"}


def test_strimzi_connectors_mirror_the_json_configs() -> None:
    for source, target in (("s3-sink.json", "s3-sink-bronze.yaml"),
                           ("s3-sink-alerts.json", "s3-sink-bronze-alerts.yaml")):
        data = _json(source)
        resource = yaml.safe_load((K8S / target).read_text())
        assert resource["kind"] == "KafkaConnector"
        assert resource["metadata"]["name"] == data["name"]
        assert resource["metadata"]["namespace"] == "kafka"
        assert resource["metadata"]["labels"]["strimzi.io/cluster"] == "market-connect"
        assert resource["spec"]["class"] == data["config"]["connector.class"]
        assert resource["spec"]["tasksMax"] == int(data["config"]["tasks.max"])
        expected = {k: v for k, v in data["config"].items()
                    if k not in {"connector.class", "tasks.max", "store.url"}}
        actual = {k: v for k, v in resource["spec"]["config"].items() if k != "store.url"}
        assert actual == expected
        assert resource["spec"]["config"]["store.url"] == "http://minio.minio.svc:9000"


def test_connect_cluster_uses_contract_names() -> None:
    connect = yaml.safe_load((K8S / "kafka-connect.yaml").read_text())
    assert connect["metadata"]["name"] == "market-connect"
    assert connect["metadata"]["annotations"]["strimzi.io/use-connector-resources"] == "true"
    assert connect["spec"]["bootstrapServers"] == "market-kafka-bootstrap:9092"
    assert connect["spec"]["image"] == "btl/kafka-connect-s3:dev"
    env = {item["name"]: item["valueFrom"]["secretKeyRef"]
           for item in connect["spec"]["template"]["connectContainer"]["env"]}
    assert env["AWS_ACCESS_KEY_ID"] == {"name": "minio-credentials", "key": "MINIO_ACCESS_KEY"}
