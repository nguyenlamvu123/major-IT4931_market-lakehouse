"""Producer lifecycle and publishing helpers shared by every market-data source.

Every source normalises raw API payloads into contract-v2 records (``common.contracts``)
and hands them to :meth:`BaseProducer.publish`, which picks the Kafka topic and key from
the contract. Sources never hard-code topic names, so a contract change only touches
``common/contracts.py`` and ``contracts/``.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Protocol

from common.contracts import TOPICS, topic_for_record

LOGGER = logging.getLogger(__name__)

_FRACTION_RE = re.compile(r"\.(\d+)")


class KafkaSink(Protocol):
    def send(self, topic: str, key: str, value: dict[str, Any]) -> None: ...

    def flush(self, timeout: float = 10.0) -> None: ...


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_to_ms(value: str) -> int:
    """Parse an RFC 3339 timestamp from a market API into epoch milliseconds UTC.

    Alpaca sends nanosecond precision (``2026-11-09T15:30:00.123456789Z``), which
    ``datetime.fromisoformat`` rejects on Python < 3.11 and truncates inconsistently
    after; the fraction is cut to microseconds before parsing.

    Before:
    - "2026-11-09T15:30:00.123456789Z"
    - "2026-11-09T15:30:00Z"

    After:
    - 1794238200123
    - 1794238200000
    """
    text = value.strip().replace("Z", "+00:00")
    text = _FRACTION_RE.sub(lambda match: "." + match.group(1)[:6].ljust(6, "0"), text, count=1)
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


class BaseProducer:
    """One market-data source publishing contract records to Kafka.

    Lifecycle: ``run(stop_event)`` blocks until ``stop_event`` is set (SIGTERM from
    Kubernetes or Ctrl+C); ``close()`` then flushes the Kafka client so in-flight
    records are delivered before the pod exits.
    """

    #: Value of the ``source`` field this producer writes (used for metrics labels).
    source = "unknown"

    def __init__(self, kafka: KafkaSink) -> None:
        self.kafka = kafka

    def run(self, stop_event: threading.Event) -> None:
        raise NotImplementedError

    def close(self) -> None:
        self.kafka.flush()

    def publish(self, record: dict[str, Any]) -> str:
        """Send one contract record to its topic, keyed as the contract says.

        Returns the topic name so callers can log or count per topic.
        """
        topic = topic_for_record(record)
        key_field = TOPICS[topic]["key"]
        self.kafka.send(topic, str(record[key_field]), record)
        return topic

    def publish_paced(
        self,
        record: dict[str, Any],
        stop_event: threading.Event,
        interval: float = 0.0,
    ) -> bool:
        """Publish then wait ``interval`` seconds; return False once a stop was requested."""
        self.publish(record)
        if interval > 0 and stop_event.wait(interval):
            return False
        return not stop_event.is_set()
