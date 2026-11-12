"""Trade sources (Kafka or JSONL files) -> cleaned, de-duplicated trades -> OHLCV bars."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import TRADE_SCHEMA


def read_kafka(
    spark: SparkSession,
    bootstrap: str,
    topics: list[str],
    starting_offsets: str = "latest",
    max_offsets_per_trigger: int | None = None,
) -> DataFrame:
    """Subscribe to the trade-schema topics and parse the JSON value.

    ``failOnDataLoss=false``: when retention (7 days) deletes offsets the checkpoint still
    points at, the query continues from the earliest available offset instead of dying.
    Those trades are not lost for the system: they are already in MinIO bronze.
    """
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", ",".join(topics))
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
    )
    if max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
    raw = reader.load()
    return raw.select(
        F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("trade")
    ).select("trade.*")


def read_files(
    spark: SparkSession,
    input_dir: str,
    topics: list[str],
    max_files_per_trigger: int = 1,
) -> DataFrame:
    """Stream ``<input_dir>/<topic>.jsonl`` files (week-1 mode on contract samples).

    Only the trade-schema topics are matched by the glob, so ``stocks.bars.jsonl`` or
    ``rates.yields.jsonl`` in the same directory are never parsed as trades.
    """
    pattern = f"{input_dir.rstrip('/')}/{{{','.join(topics)}}}.jsonl"
    return (
        spark.readStream.schema(TRADE_SCHEMA)
        .option("maxFilesPerTrigger", str(max_files_per_trigger))
        .json(pattern)
    )


def clean_trades(trades: DataFrame) -> DataFrame:
    """Drop records that violate the trade contract and add ``event_ts`` (UTC timestamp).

    Producers already validate, so this only guards against hand-made messages; a
    malformed JSON value becomes an all-null row and is dropped here.
    """
    return (
        trades.filter(F.col("trade_id").isNotNull())
        .filter(F.col("symbol").isNotNull())
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("price") > 0)
        .filter(F.col("volume") >= 0)
        .withColumn("event_ts", F.expr("timestamp_millis(event_time)"))
    )


def dedupe_trades(trades: DataFrame, watermark: str) -> DataFrame:
    """Attach the event-time watermark and drop re-sent trades by ``trade_id``.

    A producer that reconnects can re-send the last trades; without this step their
    volume would be counted twice in the bar. State for a ``trade_id`` is kept until the
    watermark passes its event time, so duplicates arriving within the watermark delay
    are dropped and the state stays bounded.
    """
    return trades.withWatermark("event_ts", watermark).dropDuplicatesWithinWatermark(
        ["trade_id"]
    )


def aggregate_ohlcv(trades: DataFrame, window_duration: str = "1 minute") -> DataFrame:
    """Event-time OHLCV bars per symbol, in the shape of ``ohlcv_1m``/``ohlcv_5m``.

    ``updated_at`` is not set here: it is the processing time of the sink write and is
    added in ``foreachBatch``. ``open``/``close`` follow event time, not arrival order.
    """
    window = F.window(F.col("event_ts"), window_duration)
    volume = F.sum("volume")
    notional = F.sum(F.col("price") * F.col("volume"))
    return (
        trades.groupBy(window.alias("window"), F.col("symbol"))
        .agg(
            # One symbol always has one asset class; max() just picks it deterministically.
            F.max("asset_class").alias("asset_class"),
            F.min_by("price", "event_ts").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "event_ts").alias("close"),
            volume.alias("volume"),
            # VWAP is undefined without volume (Yahoo DXY quotes): keep it null, not 0.
            F.when(volume > 0, notional / volume).alias("vwap"),
            F.count(F.lit(1)).alias("trade_count"),
            F.max("event_ts").alias("last_event_time"),
        )
        .select(
            "symbol",
            F.to_date(F.col("window.start")).alias("day"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "asset_class",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "trade_count",
            "last_event_time",
        )
    )
