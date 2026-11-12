"""Kafka topic list for infra tooling, read from ``contracts/topics.yaml``.

Docker Compose (``init-topics.sh``) and Kubernetes (``gen_kafka_topics.py``) both create
topics from this loader, so the contract file is the only place a topic is declared.
Run as a module to print one ``name partitions retention_ms`` line per topic:

    python -m infra.scripts.topics_contract --format cli
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
TOPICS_YAML = ROOT / "contracts" / "topics.yaml"


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    retention_ms: int
    key: str
    producer: str


def load_document(path: str | Path = TOPICS_YAML) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def load_topics(path: str | Path = TOPICS_YAML) -> list[TopicSpec]:
    document = load_document(path)
    return [
        TopicSpec(
            name=str(item["name"]),
            partitions=int(item["partitions"]),
            retention_ms=int(item["retention_ms"]),
            key=str(item["key"]),
            producer=str(item["producer"]),
        )
        for item in document["topics"]
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--format", choices=["cli"], default="cli")
    parser.add_argument("--contract", default=str(TOPICS_YAML))
    args = parser.parse_args()
    for topic in load_topics(args.contract):
        print(topic.name, topic.partitions, topic.retention_ms)


if __name__ == "__main__":
    main()
