"""Quality rules (pure) and metric collection on the synthetic silver lake (Spark)."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

from batch.quality import DatasetMetrics, evaluate

WEDNESDAY = date(2026, 11, 25)
SUNDAY = date(2026, 11, 29)
THANKSGIVING = date(2026, 11, 26)


def _healthy(day: date) -> list[DatasetMetrics]:
    newest = datetime(day.year, day.month, day.day, 23, 30, tzinfo=timezone.utc)
    return [
        DatasetMetrics("crypto.trades", 100, max_event_time=newest, input_rows=100),
        DatasetMetrics("stocks.trades", 100, input_rows=100),
        DatasetMetrics("gold.quotes", 10, input_rows=10),
        DatasetMetrics("fx.dxy", 10, input_rows=10),
        DatasetMetrics("stocks.bars", 0),
        DatasetMetrics("rates.yields", 9, trailing_rows=45),
        DatasetMetrics("alerts", 0),
    ]


def _replace(metrics: list[DatasetMetrics], dataset: str, **changes) -> list[DatasetMetrics]:
    from dataclasses import replace

    return [replace(item, **changes) if item.dataset == dataset else item for item in metrics]


def _rules(report) -> set[tuple[str, str, str]]:
    return {(f.rule, f.dataset, f.severity) for f in report.findings}


def test_healthy_weekday_passes() -> None:
    report = evaluate(WEDNESDAY, _healthy(WEDNESDAY))
    assert report.passed
    assert _rules(report) == {("non_empty", "stocks.bars", "warning")}


def test_empty_stock_topics_on_weekend_or_holiday_only_warn() -> None:
    for day in (SUNDAY, THANKSGIVING):
        metrics = _replace(_healthy(day), "stocks.trades", rows=0, input_rows=0)
        metrics = _replace(metrics, "stocks.bars", rows=0)
        report = evaluate(day, metrics)
        assert report.passed, day
        assert ("non_empty", "stocks.trades", "warning") in _rules(report)


def test_empty_stock_topic_on_market_day_fails() -> None:
    metrics = _replace(_healthy(WEDNESDAY), "stocks.trades", rows=0, input_rows=0)
    report = evaluate(WEDNESDAY, metrics)
    assert not report.passed
    assert ("non_empty", "stocks.trades", "error") in _rules(report)


def test_crypto_must_never_be_empty() -> None:
    metrics = _replace(_healthy(SUNDAY), "crypto.trades", rows=0, max_event_time=None)
    assert not evaluate(SUNDAY, metrics).passed


def test_late_yields_warn_but_missing_week_fails() -> None:
    late = _replace(_healthy(WEDNESDAY), "rates.yields", rows=0, trailing_rows=36)
    report = evaluate(WEDNESDAY, late)
    assert report.passed
    assert ("non_empty", "rates.yields", "warning") in _rules(report)

    dead = _replace(_healthy(WEDNESDAY), "rates.yields", rows=0, trailing_rows=0)
    assert ("yields_trailing", "rates.yields", "error") in _rules(evaluate(WEDNESDAY, dead))


def test_silver_invariants_are_errors() -> None:
    for field in ("null_keys", "nonpositive_prices", "bad_high_low", "duplicate_keys"):
        metrics = _replace(_healthy(WEDNESDAY), "stocks.trades", **{field: 2})
        assert not evaluate(WEDNESDAY, metrics).passed, field


def test_reject_ratio_thresholds() -> None:
    warn = _replace(_healthy(WEDNESDAY), "crypto.trades", input_rows=100, rejected_rows=10)
    assert ("reject_ratio", "crypto.trades", "warning") in _rules(evaluate(WEDNESDAY, warn))
    fail = _replace(_healthy(WEDNESDAY), "crypto.trades", input_rows=100, rejected_rows=30)
    assert not evaluate(WEDNESDAY, fail).passed


def test_stale_crypto_stream_warns() -> None:
    stale = datetime(2026, 11, 25, 12, 0, tzinfo=timezone.utc)
    metrics = _replace(_healthy(WEDNESDAY), "crypto.trades", max_event_time=stale)
    report = evaluate(WEDNESDAY, metrics)
    assert report.passed
    assert ("freshness", "crypto.trades", "warning") in _rules(report)


def test_report_serialises_to_json() -> None:
    document = json.loads(evaluate(WEDNESDAY, _healthy(WEDNESDAY)).to_json())
    assert document["dt"] == "2026-11-25"
    assert {item["dataset"] for item in document["metrics"]} >= {"crypto.trades", "alerts"}


def test_collect_metrics_on_synthetic_silver(spark, silver_lake) -> None:
    from batch.quality import collect_metrics

    metrics = {item.dataset: item for item in collect_metrics(spark, silver_lake, WEDNESDAY)}
    assert metrics["crypto.trades"].rows > 0
    assert metrics["stocks.trades"].rows > 0
    assert metrics["crypto.trades"].input_rows == metrics["crypto.trades"].rows
    assert metrics["crypto.trades"].max_event_time.tzinfo is not None
    assert metrics["rates.yields"].rows == 9
    assert metrics["rates.yields"].trailing_rows > 9
    assert all(item.duplicate_keys == 0 for item in metrics.values())

    report = evaluate(WEDNESDAY, list(metrics.values()))
    # The synthetic market has no stocks.bars and stops trading at ~14:40 UTC, so the
    # gate reports exactly those two warnings and passes.
    assert report.passed
    assert _rules(report) == {
        ("non_empty", "stocks.bars", "warning"),
        ("freshness", "crypto.trades", "warning"),
    }
