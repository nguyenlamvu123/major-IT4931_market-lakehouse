"""Prometheus metrics and throughput logging of one producer process.

Series (contract ``contracts/infra.yaml``, port 8001, path ``/metrics``):

- ``market_producer_messages_total{topic,source}``: records accepted by Kafka client
- ``market_producer_errors_total{source,kind}``: kind = connection | poll | kafka_send
- ``market_producer_reconnects_total{source}``: websocket reconnect attempts
- ``market_producer_dead_letter_total{source}``: records rejected to ``dead-letter``
- ``market_producer_duplicates_total{source}``: re-sent records dropped in-process
  (additive to the contract list; used for the PDF's "trùng tin" measurement)

Each instance owns its registry, so tests can create many producers without
"Duplicated timeseries" errors from the global registry.
"""

from __future__ import annotations

import logging
import time
from collections import Counter as TallyCounter
from collections.abc import Callable

from prometheus_client import CollectorRegistry, Counter, start_http_server

LOGGER = logging.getLogger(__name__)


class ProducerMetrics:
    def __init__(
        self,
        source: str,
        registry: CollectorRegistry | None = None,
        log_every: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.source = source
        self.registry = registry or CollectorRegistry()
        self.messages = Counter(
            "market_producer_messages", "Records sent to Kafka", ["topic", "source"],
            registry=self.registry,
        )
        self.errors = Counter(
            "market_producer_errors", "Producer errors", ["source", "kind"],
            registry=self.registry,
        )
        self.reconnects = Counter(
            "market_producer_reconnects", "Websocket reconnects", ["source"],
            registry=self.registry,
        )
        self.dead_letters = Counter(
            "market_producer_dead_letter", "Records routed to dead-letter", ["source"],
            registry=self.registry,
        )
        self.duplicates = Counter(
            "market_producer_duplicates", "Re-sent records dropped in-process", ["source"],
            registry=self.registry,
        )
        self.log_every = log_every
        self.clock = clock
        #: Per-topic counts since the last throughput log line (not exported).
        self._window: TallyCounter[str] = TallyCounter()
        self._window_started = clock()

    def message(self, topic: str) -> None:
        self.messages.labels(topic=topic, source=self.source).inc()
        self._window[topic] += 1
        self.maybe_log()

    def error(self, kind: str) -> None:
        self.errors.labels(source=self.source, kind=kind).inc()

    def reconnect(self) -> None:
        self.reconnects.labels(source=self.source).inc()

    def dead_letter(self) -> None:
        self.dead_letters.labels(source=self.source).inc()

    def duplicate(self) -> None:
        self.duplicates.labels(source=self.source).inc()

    def rates(self) -> dict[str, float]:
        """Messages per second per topic since the last log line."""
        elapsed = max(self.clock() - self._window_started, 1e-9)
        return {topic: count / elapsed for topic, count in sorted(self._window.items())}

    def maybe_log(self) -> None:
        if self.clock() - self._window_started < self.log_every:
            return
        rates = ", ".join(f"{topic}={rate:.1f}/s" for topic, rate in self.rates().items())
        LOGGER.info("source=%s throughput %s", self.source, rates or "idle")
        self._window.clear()
        self._window_started = self.clock()

    def serve(self, port: int) -> None:
        """Expose ``/metrics`` on ``port`` (daemon thread; dies with the process)."""
        start_http_server(port, registry=self.registry)
