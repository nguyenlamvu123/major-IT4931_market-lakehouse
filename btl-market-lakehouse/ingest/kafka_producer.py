"""Thin Kafka JSON producer with idempotence and a dry-run printer."""

from __future__ import annotations

import json
import logging
from typing import Any

from .config import KafkaSettings

LOGGER = logging.getLogger(__name__)


class KafkaJsonProducer:
    def __init__(self, settings: KafkaSettings) -> None:
        try:
            from confluent_kafka import Producer
        except ImportError as exc:  # pragma: no cover - depends on optional runtime
            raise RuntimeError(
                "confluent-kafka is not installed. Run: pip install confluent-kafka"
            ) from exc
        self._producer = Producer(settings.producer_config())

    @staticmethod
    def _delivery(error: Any, message: Any) -> None:
        if error is not None:
            LOGGER.error("delivery failed: %s", error)
            return
        LOGGER.debug(
            "delivered topic=%s partition=%s offset=%s",
            message.topic(),
            message.partition(),
            message.offset(),
        )

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        payload = json.dumps(value, ensure_ascii=False, default=str).encode("utf-8")
        self._producer.produce(
            topic,
            key=str(key).encode("utf-8"),
            value=payload,
            callback=self._delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)


class PrintProducer:
    """Dry-run producer used by tests and local schema inspection."""

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        print(json.dumps({"topic": topic, "key": key, "value": value}, ensure_ascii=False))

    def flush(self, timeout: float = 10.0) -> None:
        del timeout


def build_producer(settings: KafkaSettings, dry_run: bool = False) -> KafkaJsonProducer | PrintProducer:
    return PrintProducer() if dry_run else KafkaJsonProducer(settings)
