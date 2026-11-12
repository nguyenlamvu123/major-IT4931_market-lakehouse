"""Spark read schemas for bronze JSON (contracts/schemas/*.schema.json, contract v2).

Every schema ends with ``_corrupt_record``: in PERMISSIVE mode Spark puts lines that are
not valid JSON there instead of failing the job, so bronze_to_silver can count them as
rejected rows.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

CORRUPT_COLUMN = "_corrupt_record"

TRADE_SCHEMA = StructType(
    [
        StructField("trade_id", StringType()),
        StructField("symbol", StringType()),
        StructField("asset_class", StringType()),
        StructField("event_time", LongType()),
        StructField("ingest_time", LongType()),
        StructField("price", DoubleType()),
        StructField("volume", DoubleType()),
        StructField("source", StringType()),
        StructField("exchange_trade_id", StringType()),
        StructField(CORRUPT_COLUMN, StringType()),
    ]
)

BAR_SCHEMA = StructType(
    [
        StructField("bar_id", StringType()),
        StructField("symbol", StringType()),
        StructField("asset_class", StringType()),
        StructField("timeframe", StringType()),
        StructField("event_time", LongType()),
        StructField("ingest_time", LongType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("volume", DoubleType()),
        StructField("vwap", DoubleType()),
        StructField("trade_count", LongType()),
        StructField("source", StringType()),
        StructField(CORRUPT_COLUMN, StringType()),
    ]
)

RATE_SCHEMA = StructType(
    [
        StructField("series_id", StringType()),
        StructField("tenor", StringType()),
        StructField("day", StringType()),
        StructField("event_time", LongType()),
        StructField("ingest_time", LongType()),
        StructField("yield", DoubleType()),
        StructField("source", StringType()),
        StructField(CORRUPT_COLUMN, StringType()),
    ]
)

ALERT_SCHEMA = StructType(
    [
        StructField("alert_id", StringType()),
        StructField("symbol", StringType()),
        StructField("asset_class", StringType()),
        StructField("alert_time", StringType()),
        StructField("day", StringType()),
        StructField("alert_type", StringType()),
        StructField("severity", StringType()),
        StructField("price", DoubleType()),
        StructField("zscore", DoubleType()),
        StructField("volume_zscore", DoubleType()),
        StructField("message", StringType()),
        StructField(CORRUPT_COLUMN, StringType()),
    ]
)
