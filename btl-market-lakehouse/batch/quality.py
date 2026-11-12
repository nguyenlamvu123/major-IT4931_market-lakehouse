"""Data-quality gate between silver and gold (idea PDF: "chặn giữa silver và gold").

Two halves:

- :func:`evaluate` is pure Python: it turns per-dataset metrics into findings. Rules and
  thresholds live here and are unit-tested without Spark.
- :func:`collect_metrics` runs the Spark aggregations over one silver ``dt`` partition
  (pyspark is imported lazily so the rules stay testable without it).

Severity policy: an ``error`` stops the nightly DAG before gold is rebuilt from bad
data; a ``warning`` is reported (Airflow log + JSON report) but does not block, e.g. no
stock trades on a Sunday, or yields that FRED has not published yet.

Great Expectations (named in the idea PDF) can replace these rules later; the report
format below is deliberately simple so either engine can feed the dashboard.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from .lake import YIELD_LATENCY_DAYS, Lake

# NOTICE:
# NYSE full-day closures for 2026 (https://www.nyse.com/markets/hours-calendars) and the
# extra SIFMA bond-market closures (Columbus Day, Veterans Day). They only downgrade an
# "empty topic" finding from error to warning, so a missing holiday makes the DAG fail
# loudly rather than hide data loss.
# Removal condition: replace with a market-calendar library or extend every December.
NYSE_HOLIDAYS = frozenset(
    date.fromisoformat(day)
    for day in (
        "2026-01-01",
        "2026-01-19",
        "2026-02-16",
        "2026-04-03",
        "2026-05-25",
        "2026-06-19",
        "2026-07-03",
        "2026-09-07",
        "2026-11-26",
        "2026-12-25",
    )
)
BOND_ONLY_HOLIDAYS = frozenset({date(2026, 10, 12), date(2026, 11, 11)})

#: Share of bronze rows rejected by bronze_to_silver above which a finding is raised.
REJECT_WARNING_RATIO = 0.05
REJECT_ERROR_RATIO = 0.20
#: crypto.trades runs 24/7; its newest trade should be this close to the end of the day.
CRYPTO_STALE_AFTER = timedelta(hours=2)

TRADE_DATASETS = ("crypto.trades", "stocks.trades", "gold.quotes", "fx.dxy")
#: Dataset -> silver table and key columns checked for NULLs / duplicates.
DATASETS = {
    **{topic: ("trades", ("trade_id",)) for topic in TRADE_DATASETS},
    "stocks.bars": ("bars", ("bar_id",)),
    "rates.yields": ("yields", ("series_id", "day")),
    "alerts": ("alerts", ("alert_id",)),
}


@dataclass(frozen=True)
class DatasetMetrics:
    """Counts for one dataset (bronze topic) in one silver ``dt`` partition."""

    dataset: str
    rows: int
    null_keys: int = 0
    nonpositive_prices: int = 0
    bad_high_low: int = 0
    duplicate_keys: int = 0
    #: Newest event in the partition (UTC), None when empty.
    max_event_time: datetime | None = None
    #: From silver/_stats: rows read from bronze and rows rejected before silver.
    input_rows: int = 0
    rejected_rows: int = 0
    #: rates.yields only: rows in the trailing YIELD_LATENCY_DAYS window.
    trailing_rows: int = 0


@dataclass(frozen=True)
class Finding:
    rule: str
    dataset: str
    severity: str  # "error" | "warning"
    message: str
    value: float | int | None = None


@dataclass
class QualityReport:
    dt: str
    passed: bool
    findings: list[Finding] = field(default_factory=list)
    metrics: list[DatasetMetrics] = field(default_factory=list)

    def to_json(self) -> str:
        def default(value: Any) -> Any:
            if isinstance(value, datetime):
                return value.isoformat()
            raise TypeError(type(value))

        return json.dumps(asdict(self), default=default, ensure_ascii=False, sort_keys=True)


def is_market_day(day: date) -> bool:
    return day.weekday() < 5 and day not in NYSE_HOLIDAYS


def _empty_severity(dataset: str, day: date) -> tuple[str, str] | None:
    """Severity of an empty partition, or None when emptiness is expected."""
    if dataset == "alerts":
        return None  # a quiet market produces no alerts
    if dataset == "crypto.trades":
        return "error", "crypto trades 24/7; an empty day means the Binance producer was down"
    if dataset == "rates.yields":
        return "warning", "yields are published a business day late; re-read next run"
    if dataset == "stocks.bars":
        # Bars complement trades (daily falls back to trades); without an Alpaca key the
        # bar stream is legitimately silent, so this must not block the nightly run.
        return "warning", "no stock bars; daily summary falls back to trades"
    if not is_market_day(day):
        return "warning", "market closed (weekend/NYSE holiday)"
    return "error", "market day without data"


def evaluate(day: date, metrics: list[DatasetMetrics]) -> QualityReport:
    """Apply the quality rules to the metrics of silver partition ``day``."""
    findings: list[Finding] = []

    def add(rule: str, dataset: str, severity: str, message: str, value=None) -> None:
        findings.append(Finding(rule, dataset, severity, message, value))

    for item in metrics:
        if item.null_keys:
            add("null_keys", item.dataset, "error", "NULL primary key in silver", item.null_keys)
        if item.nonpositive_prices:
            add("price_positive", item.dataset, "error", "price <= 0 reached silver",
                item.nonpositive_prices)
        if item.bad_high_low:
            add("high_low", item.dataset, "error", "high < low/open/close reached silver",
                item.bad_high_low)
        if item.duplicate_keys:
            add("duplicates", item.dataset, "error", "duplicate keys after de-duplication",
                item.duplicate_keys)

        if item.rows == 0:
            expected = _empty_severity(item.dataset, day)
            if expected:
                add("non_empty", item.dataset, expected[0], expected[1], 0)
        if item.dataset == "rates.yields" and item.trailing_rows == 0:
            add("yields_trailing", item.dataset, "error",
                f"no yields at all in the last {YIELD_LATENCY_DAYS + 1} days", 0)

        if item.input_rows:
            ratio = item.rejected_rows / item.input_rows
            if ratio > REJECT_ERROR_RATIO:
                add("reject_ratio", item.dataset, "error",
                    "too many bronze rows violate the contract", round(ratio, 4))
            elif ratio > REJECT_WARNING_RATIO:
                add("reject_ratio", item.dataset, "warning",
                    "bronze rows violating the contract", round(ratio, 4))

        if item.dataset == "crypto.trades" and item.max_event_time is not None:
            end_of_day = datetime.combine(day + timedelta(days=1), datetime.min.time(),
                                          tzinfo=timezone.utc)
            newest = item.max_event_time
            if newest.tzinfo is None:
                newest = newest.replace(tzinfo=timezone.utc)
            if end_of_day - newest > CRYPTO_STALE_AFTER:
                add("freshness", item.dataset, "warning",
                    "crypto stream stopped before the end of the day",
                    round((end_of_day - newest).total_seconds() / 3600, 2))

    passed = not any(finding.severity == "error" for finding in findings)
    return QualityReport(day.isoformat(), passed, findings, metrics)


def _from_ms(millis: int | None) -> datetime | None:
    # Spark hands timestamps back as naive *local* datetimes; epoch millis are unambiguous.
    if millis is None:
        return None
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc)


def collect_metrics(spark, lake: Lake, day: date) -> list[DatasetMetrics]:
    """Aggregate silver partition ``day`` (plus silver/_stats) into DatasetMetrics."""
    from pyspark.sql import functions as F

    from .silver import read_silver

    def stats_for() -> dict[str, tuple[int, int]]:
        from .lake import existing_paths

        if not existing_paths(spark, [lake.silver("_stats")]):
            return {}
        rows = (
            spark.read.schema("topic string, input_rows long, rejected_rows long, dt date")
            .json(lake.silver("_stats"))
            .filter(F.col("dt") == F.lit(day))
            .collect()
        )
        return {row["topic"]: (row["input_rows"], row["rejected_rows"]) for row in rows}

    stats = stats_for()
    metrics: list[DatasetMetrics] = []

    trades = read_silver(spark, lake, "trades", day, day)
    by_topic = {
        row["topic"]: row
        for row in trades.groupBy("topic")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.when(F.col("trade_id").isNull() | F.col("symbol").isNull(), 1).otherwise(0))
            .alias("null_keys"),
            F.sum(F.when(F.col("price") <= 0, 1).otherwise(0)).alias("nonpositive"),
            (F.count(F.lit(1)) - F.countDistinct("trade_id")).alias("duplicates"),
            F.max("event_time_ms").alias("max_event_time_ms"),
        )
        .collect()
    }
    for topic in TRADE_DATASETS:
        row = by_topic.get(topic)
        input_rows, rejected = stats.get(topic, (0, 0))
        metrics.append(
            DatasetMetrics(
                dataset=topic,
                rows=row["rows"] if row else 0,
                null_keys=row["null_keys"] if row else 0,
                nonpositive_prices=row["nonpositive"] if row else 0,
                duplicate_keys=row["duplicates"] if row else 0,
                max_event_time=_from_ms(row["max_event_time_ms"]) if row else None,
                input_rows=input_rows,
                rejected_rows=rejected,
            )
        )

    bars = read_silver(spark, lake, "bars", day, day)
    bar_row = bars.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(F.col("bar_id").isNull(), 1).otherwise(0)).alias("null_keys"),
        F.sum(F.when(F.least("open", "high", "low", "close") <= 0, 1).otherwise(0))
        .alias("nonpositive"),
        F.sum(F.when(F.col("high") < F.greatest("open", "close", "low"), 1).otherwise(0))
        .alias("bad_high_low"),
        (F.count(F.lit(1)) - F.countDistinct("bar_id")).alias("duplicates"),
        F.max("event_time_ms").alias("max_event_time_ms"),
    ).first()
    input_rows, rejected = stats.get("stocks.bars", (0, 0))
    metrics.append(
        DatasetMetrics(
            dataset="stocks.bars",
            rows=bar_row["rows"],
            null_keys=bar_row["null_keys"] or 0,
            nonpositive_prices=bar_row["nonpositive"] or 0,
            bad_high_low=bar_row["bad_high_low"] or 0,
            duplicate_keys=bar_row["duplicates"],
            max_event_time=_from_ms(bar_row["max_event_time_ms"]),
            input_rows=input_rows,
            rejected_rows=rejected,
        )
    )

    yields = read_silver(spark, lake, "yields", day - timedelta(days=YIELD_LATENCY_DAYS), day)
    yield_row = yields.agg(
        F.sum(F.when(F.col("dt") == F.lit(day), 1).otherwise(0)).alias("rows"),
        F.count(F.lit(1)).alias("trailing"),
        F.sum(F.when(F.col("series_id").isNull() | F.col("day").isNull(), 1).otherwise(0))
        .alias("null_keys"),
        (F.count(F.lit(1)) - F.countDistinct("series_id", "day")).alias("duplicates"),
    ).first()
    input_rows, rejected = stats.get("rates.yields", (0, 0))
    metrics.append(
        DatasetMetrics(
            dataset="rates.yields",
            rows=yield_row["rows"] or 0,
            null_keys=yield_row["null_keys"] or 0,
            duplicate_keys=yield_row["duplicates"],
            input_rows=input_rows,
            rejected_rows=rejected,
            trailing_rows=yield_row["trailing"],
        )
    )

    alerts = read_silver(spark, lake, "alerts", day, day)
    alert_row = alerts.agg(
        F.count(F.lit(1)).alias("rows"),
        (F.count(F.lit(1)) - F.countDistinct("alert_id")).alias("duplicates"),
    ).first()
    input_rows, rejected = stats.get("alerts", (0, 0))
    metrics.append(
        DatasetMetrics(
            dataset="alerts",
            rows=alert_row["rows"],
            duplicate_keys=alert_row["duplicates"],
            input_rows=input_rows,
            rejected_rows=rejected,
        )
    )
    return metrics
