#!/usr/bin/env python3
"""Speed layer entrypoint: trades -> 1-minute OHLCV -> indicators/alerts -> sinks.

Call stack::

    main
      -> trade_stream (read_kafka | read_files -> clean_trades -> dedupe_trades)
      -> aggregate_ohlcv  (1 minute, event time)
      -> writeStream.foreachBatch(SpeedLayer.process_bars)
           -> Sink.write("ohlcv_1m")
           -> enrich_batch -> Sink.write("indicators_latest" / "alerts_by_day")
           -> KafkaAlertPublisher.publish  (topic "alerts")
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.streaming import StreamingQuery  # noqa: E402

from common.contracts import TRADE_TOPICS  # noqa: E402
from streaming.batch_logic import enrich_batch  # noqa: E402
from streaming.columns import OHLCV_COLUMNS  # noqa: E402
from streaming.ohlcv import (  # noqa: E402
    aggregate_ohlcv,
    clean_trades,
    dedupe_trades,
    read_files,
    read_kafka,
)
from streaming.schemas import ALERT_SCHEMA, INDICATOR_SCHEMA  # noqa: E402
from streaming.sinks import (  # noqa: E402
    CassandraSettings,
    CassandraSink,
    KafkaAlertPublisher,
    ParquetSink,
    Sink,
)

LOGGER = logging.getLogger("streaming.job")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Market speed layer (role B)")
    parser.add_argument("--source", choices=["kafka", "files"], default="kafka")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topics", default=",".join(TRADE_TOPICS))
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=0)
    parser.add_argument("--input", default="contracts/samples", help="--source files directory")
    parser.add_argument("--max-files-per-trigger", type=int, default=1)
    parser.add_argument("--checkpoint-root", default="s3a://checkpoints")
    parser.add_argument("--trigger", default="30 seconds")
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="process everything available, then stop (file mode, tests)",
    )
    # PDF Kiến trúc, "Watermark": late trades beyond this delay no longer change a bar.
    parser.add_argument("--watermark", default="30 seconds")
    parser.add_argument("--output-mode", choices=["cassandra", "parquet"], default="cassandra")
    parser.add_argument("--parquet-path", default="s3a://lake/speed")
    parser.add_argument("--cassandra-host", default=os.getenv("CASSANDRA_HOSTS", "127.0.0.1"))
    parser.add_argument("--cassandra-port", type=int, default=int(os.getenv("CASSANDRA_PORT", "9042")))
    parser.add_argument("--keyspace", default=os.getenv("CASSANDRA_KEYSPACE", "market"))
    parser.add_argument("--alert-topic", default="alerts")
    parser.add_argument("--disable-alert-kafka", action="store_true")
    return parser.parse_args(argv)


def cassandra_settings(args: argparse.Namespace) -> CassandraSettings:
    return CassandraSettings(
        host=args.cassandra_host,
        port=args.cassandra_port,
        keyspace=args.keyspace,
        username=os.getenv("CASSANDRA_USERNAME", ""),
        password=os.getenv("CASSANDRA_PASSWORD", ""),
    )


def build_spark(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder.appName("market-speed-layer")
        # Bars, `day` partitions and alert days are UTC by contract.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )
    for key, value in cassandra_settings(args).spark_options().items():
        builder = builder.config(key, value)
    # Checkpoints live in MinIO (s3a://checkpoints/...). Credentials come from the
    # minio-credentials secret through the environment, never from the command line.
    endpoint = os.getenv("MINIO_ENDPOINT")
    if endpoint:
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", ""))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", ""))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")).lower())
        )
    return builder.getOrCreate()


def build_sink(args: argparse.Namespace) -> Sink:
    if args.output_mode == "cassandra":
        return CassandraSink(cassandra_settings(args))
    return ParquetSink(args.parquet_path)


def trade_stream(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    topics = [topic for topic in args.topics.split(",") if topic]
    if args.source == "files":
        raw = read_files(spark, args.input, topics, args.max_files_per_trigger)
    else:
        raw = read_kafka(
            spark,
            args.bootstrap,
            topics,
            starting_offsets=args.starting_offsets,
            max_offsets_per_trigger=args.max_offsets_per_trigger or None,
        )
    return dedupe_trades(clean_trades(raw), args.watermark)


def _to_pandas_utc(frame: DataFrame) -> pd.DataFrame:
    pdf = frame.toPandas()
    for column in ("window_start", "window_end", "last_event_time"):
        if column in pdf.columns:
            # Arrow returns naive timestamps in the session time zone, which is UTC.
            pdf[column] = pd.to_datetime(pdf[column], utc=True)
    return pdf


class SpeedLayer:
    """``foreachBatch`` handler: writes bars, then indicators and alerts derived from them."""

    def __init__(
        self,
        spark: SparkSession,
        sink: Sink,
        alert_publisher: KafkaAlertPublisher | None = None,
    ) -> None:
        self.spark = spark
        self.sink = sink
        self.alert_publisher = alert_publisher

    def process_bars(self, batch_df: DataFrame, batch_id: int) -> None:
        bars = batch_df.withColumn("updated_at", F.current_timestamp()).select(*OHLCV_COLUMNS)
        bars.persist()
        try:
            self.sink.write("ohlcv_1m", bars)
            pdf = _to_pandas_utc(bars)
        finally:
            bars.unpersist()
        if pdf.empty:
            return
        indicators, alerts = enrich_batch(pdf, pd.Timestamp.now(tz="UTC"))
        if not indicators.empty:
            self.sink.write(
                "indicators_latest", self.spark.createDataFrame(indicators, INDICATOR_SCHEMA)
            )
        if not alerts.empty:
            alerts_df = self.spark.createDataFrame(alerts, ALERT_SCHEMA)
            self.sink.write("alerts_by_day", alerts_df)
            if self.alert_publisher is not None:
                self.alert_publisher.publish(alerts_df)
        LOGGER.info("batch=%s bars=%s alerts=%s", batch_id, len(pdf), len(alerts))


def start_query(
    spark: SparkSession, args: argparse.Namespace, layer: SpeedLayer
) -> StreamingQuery:
    bars = aggregate_ohlcv(trade_stream(spark, args), "1 minute")
    writer = (
        bars.writeStream.queryName("speed-ohlcv-1m")
        .outputMode("append")
        .foreachBatch(layer.process_bars)
        .option("checkpointLocation", f"{args.checkpoint_root.rstrip('/')}/speed-ohlcv-1m")
    )
    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=args.trigger)
    return writer.start()


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = build_spark(args)
    spark.sparkContext.setLogLevel("WARN")
    publisher = None
    if not args.disable_alert_kafka and args.source == "kafka":
        publisher = KafkaAlertPublisher(args.bootstrap, args.alert_topic)
    layer = SpeedLayer(spark, build_sink(args), publisher)
    start_query(spark, args, layer).awaitTermination()


if __name__ == "__main__":
    main()
