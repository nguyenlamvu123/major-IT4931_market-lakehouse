"""Spark schemas for Kafka trade events, OHLCV bars and alerts."""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
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

BAR_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("window_start", TimestampType(), False),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("vwap", DoubleType(), True),
        StructField("trade_count", LongType(), True),
        StructField("sma_5", DoubleType(), True),
        StructField("sma_15", DoubleType(), True),
        StructField("ema_12", DoubleType(), True),
        StructField("ema_26", DoubleType(), True),
        StructField("macd", DoubleType(), True),
        StructField("macd_signal", DoubleType(), True),
        StructField("macd_hist", DoubleType(), True),
        StructField("rsi_14", DoubleType(), True),
        StructField("return_1m", DoubleType(), True),
        StructField("volatility_30m", DoubleType(), True),
        StructField("zscore_30m", DoubleType(), True),
        StructField("volume_zscore_30m", DoubleType(), True),
    ]
)

ALERT_SCHEMA = StructType(
    [
        StructField("alert_id", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("alert_time", TimestampType(), False),
        StructField("alert_type", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("price", DoubleType(), True),
        StructField("zscore", DoubleType(), True),
        StructField("volume_zscore", DoubleType(), True),
        StructField("message", StringType(), True),
    ]
)
