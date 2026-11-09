"""Kafka -> parsed trades -> 1-minute OHLCV bars."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import TRADE_SCHEMA


def read_trades(
    spark: SparkSession,
    bootstrap: str,
    topics: list[str],
    starting_offsets: str = "latest",
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", ",".join(topics))
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_trades(kafka_df: DataFrame) -> DataFrame:
    parsed = (
        kafka_df.select(
            F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("trade")
        )
        .select("trade.*")
        .filter(F.col("symbol").isNotNull())
        .filter(F.col("price") > 0)
    )
    return parsed.withColumn(
        "event_time_ts", (F.col("event_time") / 1000).cast("timestamp")
    )


def aggregate_ohlcv(
    trades: DataFrame,
    watermark: str = "2 minutes",
    window_duration: str = "1 minute",
) -> DataFrame:
    bars = (
        trades.withWatermark("event_time_ts", watermark)
        .groupBy(F.window(F.col("event_time_ts"), window_duration), F.col("symbol"))
        .agg(
            F.expr("min_by(price, event_time_ts)").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.expr("max_by(price, event_time_ts)").alias("close"),
            F.sum("volume").alias("volume"),
            F.sum(F.col("price") * F.col("volume")).alias("notional"),
            F.count("*").alias("trade_count"),
        )
        .withColumn(
            "vwap",
            F.col("notional")
            / F.when(F.col("volume") == 0, F.lit(None)).otherwise(F.col("volume")),
        )
        .withColumn("window_start", F.col("window.start"))
        .select(
            "symbol",
            "window_start",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "trade_count",
        )
    )
    return bars
