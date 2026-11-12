"""Prometheus metrics and latency helpers of the speed layer (no Spark)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml
from prometheus_client import CollectorRegistry

from streaming.metrics import SpeedMetrics, latency_seconds, latency_stats, progress_samples

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"

KAFKA_PROGRESS = {
    "id": "8c6e7c1e-0000-0000-0000-000000000000",
    "name": "speed-ohlcv-1m",
    "batchId": 42,
    "inputRowsPerSecond": 812.5,
    "processedRowsPerSecond": 1650.0,
    "durationMs": {"triggerExecution": 731, "addBatch": 512},
    "sources": [
        {
            "description": "KafkaV2[Subscribe[crypto.trades,stocks.trades]]",
            "metrics": {
                "avgOffsetsBehindLatest": "12.0",
                "maxOffsetsBehindLatest": "40",
                "minOffsetsBehindLatest": "0",
            },
        }
    ],
}


def test_progress_samples_from_kafka_source() -> None:
    samples = progress_samples(KAFKA_PROGRESS)
    assert samples == {
        "input_rows_per_second": 812.5,
        "processed_rows_per_second": 1650.0,
        "batch_duration_ms": 731.0,
        "offsets_behind_latest": 40.0,
    }


def test_progress_samples_without_kafka_metrics() -> None:
    progress = {"name": "speed-ohlcv-5m", "inputRowsPerSecond": "NaN", "sources": [{}]}
    samples = progress_samples(progress)
    assert samples["offsets_behind_latest"] is None
    assert samples["input_rows_per_second"] is None
    assert samples["batch_duration_ms"] is None


def test_gauges_use_contract_names_and_query_label() -> None:
    registry = CollectorRegistry()
    metrics = SpeedMetrics(registry)
    metrics.record_progress(KAFKA_PROGRESS)
    metrics.record_latency("speed-ohlcv-1m", pd.Series([1.0, 2.0, 3.0, 4.0, 20.0]))

    labels = {"query": "speed-ohlcv-1m"}
    assert registry.get_sample_value("market_speed_input_rows_per_second", labels) == 812.5
    assert registry.get_sample_value("market_speed_processed_rows_per_second", labels) == 1650.0
    assert registry.get_sample_value("market_speed_batch_duration_ms", labels) == 731.0
    assert registry.get_sample_value("market_speed_offsets_behind_latest", labels) == 40.0
    assert registry.get_sample_value("market_speed_event_to_sink_latency_seconds", labels) == 16.8

    infra = yaml.safe_load((CONTRACTS / "infra.yaml").read_text(encoding="utf-8"))
    driver = infra["metrics"]["streaming_driver"]
    assert driver["port"] == 9108
    exported = {metric.name for metric in registry.collect()}
    assert set(driver["series"]) <= exported


def test_missing_progress_values_do_not_reset_gauges() -> None:
    registry = CollectorRegistry()
    metrics = SpeedMetrics(registry)
    metrics.record_progress(KAFKA_PROGRESS)
    metrics.record_progress({"name": "speed-ohlcv-1m", "sources": [{"metrics": {}}]})
    labels = {"query": "speed-ohlcv-1m"}
    assert registry.get_sample_value("market_speed_offsets_behind_latest", labels) == 40.0


def test_latency_from_bar_timestamps() -> None:
    bars = pd.DataFrame(
        {
            "updated_at": pd.to_datetime(["2026-11-09T14:30:07", "2026-11-09T14:31:03"]),
            "last_event_time": pd.to_datetime(["2026-11-09T14:30:02", "2026-11-09T14:31:01"]),
        }
    )
    latencies = latency_seconds(bars)
    assert list(latencies) == [5.0, 2.0]
    stats = latency_stats(latencies)
    assert stats["count"] == 2
    assert stats["max"] == 5.0
    assert latency_stats(pd.Series(dtype=float))["p95"] is None
