"""Prometheus metrics of the speed layer (contract ``contracts/infra.yaml`` -> metrics).

Exposed by the driver on port 9108 (``/metrics``), one series per streaming query
(label ``query`` = ``speed-ohlcv-1m`` / ``speed-ohlcv-5m``):

- ``market_speed_input_rows_per_second``: trades arriving from Kafka/files.
- ``market_speed_processed_rows_per_second``: trades Spark processed.
- ``market_speed_batch_duration_ms``: wall time of the last micro-batch.
- ``market_speed_offsets_behind_latest``: Kafka messages not read yet (max over topic
  partitions). Spark keeps its offsets in the checkpoint, not in a Kafka consumer group,
  so kafka-exporter cannot show this lag; the Kafka source reports it instead.
- ``market_speed_event_to_sink_latency_seconds``: p95 over the bars of the last batch
  of ``updated_at - last_event_time`` (trade time -> Cassandra upsert).

Only ``prometheus_client`` is imported here so the pure helpers can be used by the
offline tools without a JVM.
"""

from __future__ import annotations

import logging
import math
from typing import Any

import pandas as pd
from prometheus_client import REGISTRY, CollectorRegistry, Gauge, start_http_server

LOGGER = logging.getLogger(__name__)


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def progress_samples(progress: dict[str, Any]) -> dict[str, float | None]:
    """Extract gauge values from a ``StreamingQueryProgress`` JSON document.

    ``maxOffsetsBehindLatest`` is reported by the Kafka source (Spark >= 3.4) as a string
    in ``sources[].metrics``; file sources do not report it, which yields ``None``.
    """
    behind: list[float] = []
    for source in progress.get("sources") or []:
        value = _number((source.get("metrics") or {}).get("maxOffsetsBehindLatest"))
        if value is not None:
            behind.append(value)
    duration = progress.get("batchDuration")
    if duration is None:
        duration = (progress.get("durationMs") or {}).get("triggerExecution")
    return {
        "input_rows_per_second": _number(progress.get("inputRowsPerSecond")),
        "processed_rows_per_second": _number(progress.get("processedRowsPerSecond")),
        "batch_duration_ms": _number(duration),
        "offsets_behind_latest": max(behind) if behind else None,
    }


def latency_seconds(bars: pd.DataFrame) -> pd.Series:
    """Per-bar delay from its newest trade to the sink write, in seconds.

    ``updated_at`` is the micro-batch's write timestamp (taken when the batch starts
    writing), so this slightly under-estimates the moment the row is visible in
    Cassandra; the bias is bounded by the write duration of one batch.
    """
    if bars.empty:
        return pd.Series(dtype=float)
    updated = pd.to_datetime(bars["updated_at"], utc=True)
    last_event = pd.to_datetime(bars["last_event_time"], utc=True)
    return (updated - last_event).dt.total_seconds().dropna()


def latency_stats(latencies: pd.Series) -> dict[str, float | int | None]:
    """p50/p95/max over a latency series, as reported in docs/metrics."""
    if latencies.empty:
        return {"count": 0, "p50": None, "p95": None, "max": None}
    return {
        "count": int(latencies.size),
        "p50": round(float(latencies.quantile(0.5)), 3),
        "p95": round(float(latencies.quantile(0.95)), 3),
        "max": round(float(latencies.max()), 3),
    }


class SpeedMetrics:
    """The contract gauges, registered on ``registry`` (a private one in tests)."""

    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self.registry = registry
        labels = ["query"]
        self.gauges = {
            "input_rows_per_second": Gauge(
                "market_speed_input_rows_per_second",
                "Rows per second arriving at the streaming source",
                labels,
                registry=registry,
            ),
            "processed_rows_per_second": Gauge(
                "market_speed_processed_rows_per_second",
                "Rows per second processed by the streaming query",
                labels,
                registry=registry,
            ),
            "batch_duration_ms": Gauge(
                "market_speed_batch_duration_ms",
                "Duration of the last micro-batch in milliseconds",
                labels,
                registry=registry,
            ),
            "offsets_behind_latest": Gauge(
                "market_speed_offsets_behind_latest",
                "Kafka offsets not yet read (max over partitions)",
                labels,
                registry=registry,
            ),
        }
        self.latency = Gauge(
            "market_speed_event_to_sink_latency_seconds",
            "p95 delay from the newest trade of a bar to its sink write, last batch",
            labels,
            registry=registry,
        )

    def record_progress(self, progress: dict[str, Any]) -> None:
        query = progress.get("name") or str(progress.get("id", "unknown"))
        for key, value in progress_samples(progress).items():
            # Absent values (e.g. no Kafka source) leave the previous sample untouched
            # instead of exporting a misleading 0.
            if value is not None:
                self.gauges[key].labels(query=query).set(value)

    def record_latency(self, query: str, latencies: pd.Series) -> None:
        stats = latency_stats(latencies)
        if stats["p95"] is not None:
            self.latency.labels(query=query).set(stats["p95"])


def make_progress_listener(metrics: SpeedMetrics):
    """Build a ``StreamingQueryListener`` feeding ``metrics`` (imports pyspark lazily)."""
    import json

    from pyspark.sql.streaming import StreamingQueryListener

    class PrometheusProgressListener(StreamingQueryListener):
        def onQueryStarted(self, event) -> None:  # noqa: N802 - Spark API name
            LOGGER.info("query started: %s", event.name)

        def onQueryProgress(self, event) -> None:  # noqa: N802
            metrics.record_progress(json.loads(event.progress.json))

        def onQueryIdle(self, event) -> None:  # noqa: N802
            return None

        def onQueryTerminated(self, event) -> None:  # noqa: N802
            LOGGER.warning("query terminated: %s exception=%s", event.id, event.exception)

    return PrometheusProgressListener()


def serve(port: int, registry: CollectorRegistry = REGISTRY) -> None:
    """Start the ``/metrics`` HTTP endpoint in a daemon thread of the driver."""
    start_http_server(port, registry=registry)
    LOGGER.info("prometheus metrics on :%s/metrics", port)
