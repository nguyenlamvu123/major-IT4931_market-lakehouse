"""Bronze (raw JSON from Kafka Connect) -> silver (typed, validated, de-duplicated Parquet).

Silver tables and their de-duplication key (contracts/paths.yaml):

| table  | bronze topics                                         | key                  |
|--------|-------------------------------------------------------|----------------------|
| trades | crypto.trades, stocks.trades, gold.quotes, fx.dxy     | trade_id             |
| bars   | stocks.bars                                           | bar_id               |
| yields | rates.yields                                          | (series_id, day)     |
| alerts | alerts                                                | alert_id             |

Every bronze row gets a ``reject_reason`` (NULL = valid). Rejected rows never reach
silver; their counts per reason are written to ``silver/_stats/dt=…`` so the quality
job and the report can show "% bản ghi bị chặn" (lessons learned 9 in the idea PDF).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StructType

from common.contracts import (
    ALERT_TYPES,
    ASSET_CLASSES,
    BAR_TIMEFRAMES,
    RATE_SERIES,
    SEVERITIES,
    SYMBOL_PATTERN,
    TRADE_TOPICS,
)

from .lake import Lake, existing_paths
from .schemas import ALERT_SCHEMA, BAR_SCHEMA, CORRUPT_COLUMN, RATE_SCHEMA, TRADE_SCHEMA

TRADE_ASSET_CLASSES = [name for name in ASSET_CLASSES if name != "rates"]

TRADES_COLUMNS = [
    "trade_id",
    "topic",
    "symbol",
    "asset_class",
    "source",
    "exchange_trade_id",
    "event_time",
    "event_time_ms",
    "ingest_time",
    "price",
    "volume",
    "dt",
]
BARS_COLUMNS = [
    "bar_id",
    "symbol",
    "asset_class",
    "timeframe",
    "source",
    "event_time",
    "event_time_ms",
    "ingest_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trade_count",
    "dt",
]
YIELDS_COLUMNS = ["series_id", "tenor", "day", "event_time", "yield", "source", "ingest_time", "dt"]
ALERTS_COLUMNS = [
    "alert_id",
    "symbol",
    "asset_class",
    "alert_time",
    "day",
    "alert_type",
    "severity",
    "price",
    "zscore",
    "volume_zscore",
    "message",
    "dt",
]

#: Silver schemas as DDL, used to build empty frames when a table has no data yet
#: (e.g. no stocks.bars were ever produced) so gold jobs do not fail on a missing path.
SILVER_DDL = {
    "trades": (
        "trade_id string, topic string, symbol string, asset_class string, source string, "
        "exchange_trade_id string, event_time timestamp, event_time_ms long, "
        "ingest_time long, price double, volume double, dt date"
    ),
    "bars": (
        "bar_id string, symbol string, asset_class string, timeframe string, source string, "
        "event_time timestamp, event_time_ms long, ingest_time long, open double, "
        "high double, low double, close double, volume double, vwap double, "
        "trade_count long, dt date"
    ),
    "yields": (
        "series_id string, tenor string, day date, event_time timestamp, yield double, "
        "source string, ingest_time long, dt date"
    ),
    "alerts": (
        "alert_id string, symbol string, asset_class string, alert_time timestamp, "
        "day date, alert_type string, severity string, price double, zscore double, "
        "volume_zscore double, message string, dt date"
    ),
}

#: Directory name pattern written by the Kafka Connect TimeBasedPartitioner.
_BRONZE_DT_RE = r"dt=(\d{4}-\d{2}-\d{2})"


@dataclass(frozen=True)
class TopicStats:
    """Row counts of one bronze topic for one bronze ``dt`` partition."""

    topic: str
    dt: str
    input_rows: int
    rejected_rows: int
    rejected_by_reason: dict[str, int]
    duplicate_rows: int
    output_rows: int


def read_bronze(
    spark: SparkSession, lake: Lake, topic: str, days: Iterable[date], schema: StructType
) -> DataFrame:
    """Read ``bronze/<topic>/dt=<day>/`` for each requested day that exists.

    Reading the bronze root in one call is not possible: Spark's partition discovery
    sees ``<topic>/dt=…`` leaf directories under several different base paths and
    aborts with "Conflicting directory structures detected". Each topic is read on its
    own, and the bronze partition each row came from is kept in ``_bronze_dt``.
    """
    paths = existing_paths(spark, [lake.bronze(topic, day) for day in days])
    if not paths:
        empty = spark.createDataFrame([], schema)
        return empty.withColumn("_bronze_dt", F.lit(None).cast(DateType()))
    raw = (
        spark.read.schema(schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .json(paths)
    )
    return raw.withColumn(
        "_bronze_dt", F.to_date(F.regexp_extract(F.input_file_name(), _BRONZE_DT_RE, 1))
    )


def _first_reason(*rules: tuple[Column, str]) -> Column:
    """NULL when every rule passes, else the label of the first failing rule."""
    reason = F.lit(None).cast("string")
    for failed, label in reversed(rules):
        reason = F.when(failed, F.lit(label)).otherwise(reason)
    return reason


def _bad_symbol() -> Column:
    return F.col("symbol").isNull() | ~F.col("symbol").rlike(SYMBOL_PATTERN.pattern)


def _not_in(column: str, allowed: list[str]) -> Column:
    # isin() on NULL yields NULL, which F.when treats as "rule passed"; spell it out.
    return F.col(column).isNull() | ~F.col(column).isin(allowed)


def _dt_mismatch() -> Column:
    # A row whose event date differs from the bronze partition it was read from would be
    # written into another silver partition. With dynamic partition overwrite that would
    # replace a day we were not asked to rebuild with a partial copy, so it is rejected.
    return F.col("dt").isNull() | (F.col("dt") != F.col("_bronze_dt"))


def check_trades(raw: DataFrame, topic: str) -> DataFrame:
    event_time = F.timestamp_millis(F.col("event_time"))
    checked = (
        raw.withColumn("topic", F.lit(topic))
        .withColumn("event_time_ms", F.col("event_time"))
        .withColumn("event_time", event_time)
        # Resolve against the replaced column: event_time is a timestamp from here on.
        .withColumn("dt", F.to_date("event_time"))
    )
    return checked.withColumn(
        "reject_reason",
        _first_reason(
            (F.col(CORRUPT_COLUMN).isNotNull(), "malformed_json"),
            (F.col("trade_id").isNull(), "missing_trade_id"),
            (_bad_symbol(), "bad_symbol"),
            (_not_in("asset_class", TRADE_ASSET_CLASSES), "bad_asset_class"),
            (F.col("event_time_ms").isNull(), "missing_event_time"),
            (F.col("price").isNull() | (F.col("price") <= 0) | F.isnan("price"), "bad_price"),
            (F.col("volume").isNull() | (F.col("volume") < 0) | F.isnan("volume"), "bad_volume"),
            (_dt_mismatch(), "dt_mismatch"),
        ),
    )


def check_bars(raw: DataFrame) -> DataFrame:
    event_time = F.timestamp_millis(F.col("event_time"))
    checked = (
        raw.withColumn("event_time_ms", F.col("event_time"))
        .withColumn("event_time", event_time)
        # Resolve against the replaced column: event_time is a timestamp from here on.
        .withColumn("dt", F.to_date("event_time"))
    )
    prices = [F.col(name) for name in ("open", "high", "low", "close")]
    nonpositive = F.lit(False)
    for price in prices:
        nonpositive = nonpositive | price.isNull() | (price <= 0)
    bad_high_low = (F.col("high") < F.greatest("open", "close", "low")) | (
        F.col("low") > F.least("open", "close")
    )
    return checked.withColumn(
        "reject_reason",
        _first_reason(
            (F.col(CORRUPT_COLUMN).isNotNull(), "malformed_json"),
            (F.col("bar_id").isNull(), "missing_bar_id"),
            (_bad_symbol(), "bad_symbol"),
            (_not_in("asset_class", TRADE_ASSET_CLASSES), "bad_asset_class"),
            (_not_in("timeframe", list(BAR_TIMEFRAMES)), "bad_timeframe"),
            (F.col("event_time_ms").isNull(), "missing_event_time"),
            (nonpositive, "bad_price"),
            (bad_high_low, "bad_high_low"),
            (F.col("volume").isNull() | (F.col("volume") < 0), "bad_volume"),
            (_dt_mismatch(), "dt_mismatch"),
        ),
    )


def check_yields(raw: DataFrame) -> DataFrame:
    series_to_tenor = F.create_map(
        *[item for pair in RATE_SERIES.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )
    day = F.when(F.col("day").rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date("day"))
    checked = (
        raw.withColumn("day", day)
        .withColumn("dt", F.col("day"))
        .withColumn("event_time", F.timestamp_millis(F.col("event_time")))
    )
    return checked.withColumn(
        "reject_reason",
        _first_reason(
            (F.col(CORRUPT_COLUMN).isNotNull(), "malformed_json"),
            (_not_in("series_id", list(RATE_SERIES)), "bad_series_id"),
            (
                F.col("tenor").isNull() | (series_to_tenor[F.col("series_id")] != F.col("tenor")),
                "tenor_mismatch",
            ),
            (F.col("day").isNull(), "bad_day"),
            (F.col("yield").isNull() | F.isnan("yield"), "bad_yield"),
            (_dt_mismatch(), "dt_mismatch"),
        ),
    )


def check_alerts(raw: DataFrame) -> DataFrame:
    alert_time = F.to_timestamp("alert_time")
    checked = (
        raw.withColumn("alert_time", alert_time)
        .withColumn("dt", F.to_date(alert_time))
        .withColumn("day", F.col("dt"))
    )
    return checked.withColumn(
        "reject_reason",
        _first_reason(
            (F.col(CORRUPT_COLUMN).isNotNull(), "malformed_json"),
            (F.col("alert_id").isNull(), "missing_alert_id"),
            (_bad_symbol(), "bad_symbol"),
            (F.col("alert_time").isNull(), "bad_alert_time"),
            (_not_in("alert_type", list(ALERT_TYPES)), "bad_alert_type"),
            (_not_in("severity", list(SEVERITIES)), "bad_severity"),
            (_dt_mismatch(), "dt_mismatch"),
        ),
    )


def dedupe(valid: DataFrame, keys: list[str], order: list[Column]) -> DataFrame:
    """Keep one row per key, chosen deterministically by ``order``.

    ``dropDuplicates`` keeps an arbitrary row, which would make re-runs produce
    different silver files for the same bronze input.
    """
    window = Window.partitionBy(*keys).orderBy(*order)
    return (
        valid.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def _stats(topic: str, checked: DataFrame, output: DataFrame) -> list[TopicStats]:
    by_reason = checked.groupBy("_bronze_dt", "reject_reason").count().collect()
    output_counts = {
        row["dt"].isoformat(): row["count"] for row in output.groupBy("dt").count().collect()
    }
    per_day: dict[str, dict[str, int]] = {}
    for row in by_reason:
        if row["_bronze_dt"] is None:
            continue
        day = row["_bronze_dt"].isoformat()
        per_day.setdefault(day, {})[row["reject_reason"] or ""] = row["count"]
    stats: list[TopicStats] = []
    for day, reasons in sorted(per_day.items()):
        valid = reasons.pop("", 0)
        out = output_counts.get(day, 0)
        stats.append(
            TopicStats(
                topic=topic,
                dt=day,
                input_rows=valid + sum(reasons.values()),
                rejected_rows=sum(reasons.values()),
                rejected_by_reason=dict(sorted(reasons.items())),
                duplicate_rows=valid - out,
                output_rows=out,
            )
        )
    return stats


def build_silver(
    spark: SparkSession, lake: Lake, days: list[date]
) -> tuple[dict[str, DataFrame], list[TopicStats]]:
    """Read bronze for ``days`` and return the four silver DataFrames plus row stats."""
    stats: list[TopicStats] = []

    trade_parts: list[DataFrame] = []
    for topic in TRADE_TOPICS:
        checked = check_trades(read_bronze(spark, lake, topic, days, TRADE_SCHEMA), topic).cache()
        valid = checked.filter(F.col("reject_reason").isNull())
        # A trade re-sent after a producer reconnect keeps its trade_id; the first copy
        # that reached Kafka wins.
        output = dedupe(valid, ["trade_id"], [F.col("ingest_time").asc_nulls_last()])
        output = output.select(TRADES_COLUMNS)
        stats.extend(_stats(topic, checked, output))
        trade_parts.append(output)
    trades = trade_parts[0]
    for part in trade_parts[1:]:
        trades = trades.unionByName(part)

    checked_bars = check_bars(read_bronze(spark, lake, "stocks.bars", days, BAR_SCHEMA)).cache()
    bars = dedupe(
        checked_bars.filter(F.col("reject_reason").isNull()),
        ["bar_id"],
        [F.col("ingest_time").asc_nulls_last()],
    ).select(BARS_COLUMNS)
    stats.extend(_stats("stocks.bars", checked_bars, bars))

    checked_yields = check_yields(
        read_bronze(spark, lake, "rates.yields", days, RATE_SCHEMA)
    ).cache()
    # FRED revises recent observations; the most recently ingested value for a
    # (series, day) is the one to keep.
    yields = dedupe(
        checked_yields.filter(F.col("reject_reason").isNull()),
        ["series_id", "day"],
        [F.col("ingest_time").desc_nulls_last()],
    ).select(YIELDS_COLUMNS)
    stats.extend(_stats("rates.yields", checked_yields, yields))

    checked_alerts = check_alerts(read_bronze(spark, lake, "alerts", days, ALERT_SCHEMA)).cache()
    alerts = dedupe(
        checked_alerts.filter(F.col("reject_reason").isNull()), ["alert_id"], [F.col("alert_time")]
    ).select(ALERTS_COLUMNS)
    stats.extend(_stats("alerts", checked_alerts, alerts))

    return {"trades": trades, "bars": bars, "yields": yields, "alerts": alerts}, stats


def write_silver(
    spark: SparkSession, lake: Lake, tables: dict[str, DataFrame], stats: list[TopicStats]
) -> None:
    """Overwrite the ``dt`` partitions present in each table (dynamic overwrite).

    A day with zero valid rows writes nothing, so an older partition for that day is
    kept; re-running only ever replaces days that produced data.
    """
    for name, frame in tables.items():
        frame.write.mode("overwrite").partitionBy("dt").parquet(lake.silver(name))
    if stats:
        rows = [
            {
                "topic": item.topic,
                "dt": date.fromisoformat(item.dt),
                "input_rows": item.input_rows,
                "rejected_rows": item.rejected_rows,
                "rejected_by_reason": item.rejected_by_reason,
                "duplicate_rows": item.duplicate_rows,
                "output_rows": item.output_rows,
            }
            for item in stats
        ]
        schema = (
            "topic string, dt date, input_rows long, rejected_rows long, "
            "rejected_by_reason map<string,long>, duplicate_rows long, output_rows long"
        )
        (
            spark.createDataFrame(rows, schema)
            .coalesce(1)
            .write.mode("overwrite")
            .partitionBy("dt")
            .json(lake.silver("_stats"))
        )


def read_silver(
    spark: SparkSession, lake: Lake, table: str, first: date, last: date
) -> DataFrame:
    """Read silver ``table`` for ``first <= dt <= last``.

    The filter is on the ``dt`` partition column, so Spark prunes whole directories
    instead of scanning the table (measured in jobs/benchmark.py).
    """
    columns = [item.split()[0] for item in SILVER_DDL[table].split(", ")]
    if not existing_paths(spark, [lake.silver(table)]):
        return spark.createDataFrame([], SILVER_DDL[table])
    # An explicit schema skips Parquet footer inference and also works for a table
    # directory that exists but holds no files yet (a day that produced zero rows).
    frame = spark.read.schema(SILVER_DDL[table]).parquet(lake.silver(table))
    return frame.filter(F.col("dt").between(F.lit(first), F.lit(last))).select(columns)
