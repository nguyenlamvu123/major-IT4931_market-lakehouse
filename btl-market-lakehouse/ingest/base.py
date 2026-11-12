"""Producer lifecycle and publishing helpers shared by every market-data source.

Every source normalises raw API payloads into contract-v2 records (``common.contracts``)
and hands them to :meth:`BaseProducer.publish`, which owns the publishing policy:

1. route: topic and Kafka key come from the contract (``topic_for_record``);
2. validate: the topic's schema validator runs; violations go to ``dead-letter``;
3. de-duplicate: ids re-sent after a reconnect are dropped (bounded memory);
4. send and count (Prometheus, msgs/s log line).

Sources never hard-code topic names, so a contract change only touches
``common/contracts.py`` and ``contracts/``.
"""

from __future__ import annotations

import asyncio
import logging
import re
import threading
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any, Protocol

from common.contracts import TOPICS, topic_for_record, validate_bar, validate_rate, validate_trade

from .dedupe import RecentIds
from .metrics import ProducerMetrics
from .resilience import Backoff, CircuitBreaker

LOGGER = logging.getLogger(__name__)

DEAD_LETTER_TOPIC = "dead-letter"
_FRACTION_RE = re.compile(r"\.(\d+)")
_VALIDATORS: dict[str, Callable[[dict[str, Any]], list[str]]] = {
    "trade": validate_trade,
    "bar": validate_bar,
    "rate_yield": validate_rate,
}


class KafkaSink(Protocol):
    def send(self, topic: str, key: str, value: dict[str, Any]) -> None: ...

    def flush(self, timeout: float = 10.0) -> None: ...


class MalformedPayload(ValueError):
    """An API message of the expected type that cannot be turned into a record."""


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


def _record_id(record: dict[str, Any]) -> str | None:
    return record.get("trade_id") or record.get("bar_id")


class BaseProducer:
    """One market-data source publishing contract records to Kafka.

    Lifecycle: ``run(stop_event)`` blocks until ``stop_event`` is set (SIGTERM from
    Kubernetes or Ctrl+C); ``close()`` then flushes the Kafka client so in-flight
    records are delivered before the pod exits.
    """

    #: Value of the ``source`` field this producer writes (metrics label, DLQ key).
    source = "unknown"
    #: Ids remembered for in-process de-duplication; 0 disables it (mock/replay loops
    #: intentionally re-send identical records).
    dedupe_window = 50_000

    def __init__(self, kafka: KafkaSink, metrics: ProducerMetrics | None = None) -> None:
        self.kafka = kafka
        self.metrics = metrics or ProducerMetrics(self.source)
        self.recent_ids = RecentIds(self.dedupe_window) if self.dedupe_window else None

    def run(self, stop_event: threading.Event) -> None:
        raise NotImplementedError

    def close(self) -> None:
        self.kafka.flush()

    def dead_letter(self, payload: Any, error: str, topic: str | None = None) -> None:
        """Route a rejected record/payload to ``dead-letter`` instead of dropping it."""
        envelope = {
            "source": self.source,
            "topic": topic or "unknown",
            "error": error,
            "payload": payload,
            "ingest_time": now_ms(),
        }
        self.kafka.send(DEAD_LETTER_TOPIC, self.source, envelope)
        self.metrics.dead_letter()
        LOGGER.warning("dead-letter source=%s topic=%s error=%s", self.source, topic, error)

    def publish(self, record: dict[str, Any]) -> str | None:
        """Validate and send one contract record; return its topic, or None if not sent."""
        try:
            topic = topic_for_record(record)
        except KeyError as exc:
            self.dead_letter(record, f"unroutable: {exc}")
            return None
        validator = _VALIDATORS.get(TOPICS[topic]["schema"])
        errors = validator(record) if validator else []
        if errors:
            self.dead_letter(record, "; ".join(errors), topic)
            return None
        record_id = _record_id(record)
        if self.recent_ids is not None and record_id and self.recent_ids.seen(record_id):
            self.metrics.duplicate()
            return None
        key_field = TOPICS[topic]["key"]
        try:
            self.kafka.send(topic, str(record[key_field]), record)
        except Exception:
            # BufferError (local queue full) or KafkaException: count, then let the run
            # loop decide (reconnect/backoff) instead of silently losing the record.
            self.metrics.error("kafka_send")
            raise
        self.metrics.message(topic)
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

    def run_with_reconnect(
        self,
        consume: Callable[[threading.Event], Awaitable[None]],
        stop_event: threading.Event,
        backoff: Backoff | None = None,
        healthy_after: float = 60.0,
        wait: Callable[[float], bool] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Keep a websocket session alive until ``stop_event`` is set.

        ``consume`` returns normally only after a stop request; any exception (DNS,
        refused connection, ``ConnectionClosed``, exchange error frame) counts as a
        disconnect. The next attempt waits ``backoff.next_delay()``; a session that
        lasted ``healthy_after`` seconds resets the backoff, so a feed that drops once a
        day reconnects in ~1 s while a feed that is down keeps doubling up to the cap.
        """
        backoff = backoff or Backoff()
        wait = wait or stop_event.wait
        while not stop_event.is_set():
            started = clock()
            try:
                asyncio.run(consume(stop_event))
                if stop_event.is_set():
                    return
                raise ConnectionError("stream ended without stop request")
            except Exception as exc:  # noqa: BLE001 - any failure means reconnect
                if stop_event.is_set():
                    return
                self.metrics.error("connection")
                self.metrics.reconnect()
                if clock() - started >= healthy_after:
                    backoff.reset()
                delay = backoff.next_delay()
                LOGGER.warning(
                    "source=%s disconnected (%s); reconnect in %.1fs", self.source, exc, delay
                )
                if wait(delay):
                    return

    def poll_forever(
        self,
        poll_once: Callable[[], int],
        interval: float,
        stop_event: threading.Event,
        breaker: CircuitBreaker | None = None,
    ) -> None:
        """Run ``poll_once`` every ``interval`` seconds behind a circuit breaker.

        While the breaker is open the poll is skipped (the API is not called), which
        protects rate limits during an outage; the half-open trial decides when to
        resume.
        """
        breaker = breaker or CircuitBreaker(failure_threshold=3, reset_timeout=max(60.0, interval))
        while not stop_event.is_set():
            if breaker.allow():
                try:
                    breaker.call(poll_once)
                except Exception as exc:  # noqa: BLE001 - keep the poller alive
                    self.metrics.error("poll")
                    LOGGER.warning(
                        "source=%s poll failed (%s); breaker=%s", self.source, exc, breaker.state
                    )
            else:
                LOGGER.info("source=%s breaker open, skipping poll", self.source)
            stop_event.wait(interval)
