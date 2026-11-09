"""Spark schemas for bronze inputs and rate ticks."""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

TRADE_SCHEMA = StructType(
    [
        StructField("trade_id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("asset_class", StringType(), True),
        StructField("event_time", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("source", StringType(), True),
        StructField("ingest_time", LongType(), True),
    ]
)

RATE_SCHEMA = StructType(
    [
        StructField("day", StringType(), True),
        StructField("tenor", StringType(), True),
        StructField("yield", DoubleType(), True),
        StructField("source", StringType(), True),
        StructField("event_time", LongType(), True),
    ]
)
