#!/usr/bin/env python3
"""Speed layer entrypoint: trades -> 1m/5m OHLCV -> indicators/alerts -> sinks.

Two independent streaming queries read the same trade topics, each with its own
checkpoint under ``s3a://checkpoints/<query>/``:

    main
      -> start_queries
           -> trade_stream (read_kafka | read_files -> clean_trades -> dedupe_trades)
           -> aggregate_ohlcv("1 minute")  -> foreachBatch(SpeedLayer.handler(1m))
                -> Sink.write("ohlcv_1m") -> IndicatorState.update (bars kept across batches)
                -> Sink.write("indicators_latest" / "alerts_by_day")
                -> KafkaAlertPublisher.publish (topic "alerts")
           -> aggregate_ohlcv("5 minutes") -> foreachBatch(SpeedLayer.handler(5m))
                -> Sink.write("ohlcv_5m")

Why output mode "update" (PDF: "nến 1 phút có trong Cassandra chậm dưới khoảng 10 giây"):
in "append" mode Spark emits a window only once the watermark (max event time minus
the watermark delay) has passed the window end. A 10:00-10:01 bar would reach Cassandra
no earlier than when a trade stamped 10:01:30 arrives, i.e. 30-90 s after its last
trade (the v1 job used a 2-minute watermark: 2-3 minutes). In "update" mode every
trigger (default 5 s) emits the bars that changed, including the still-open one, and the
sink upserts them by primary key; the final upsert happens when the watermark closes
the window, after which late trades are dropped.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.streaming import StreamingQuery  # noqa: E402

from common.contracts import TRADE_TOPICS  # noqa: E402
from streaming.columns import OHLCV_COLUMNS  # noqa: E402
from streaming.metrics import (  # noqa: E402
    SpeedMetrics,
    latency_seconds,
    make_progress_listener,
    serve,
)
from streaming.ohlcv import (  # noqa: E402
    aggregate_ohlcv,
    clean_trades,
    dedupe_trades,
    read_files,
    read_kafka,
)
from streaming.schemas import ALERT_SCHEMA, INDICATOR_SCHEMA  # noqa: E402
from streaming.sinks import (  # noqa: E402
    CassandraBarLoader,
    CassandraSettings,
    CassandraSink,
    KafkaAlertPublisher,
    ParquetSink,
    Sink,
)
from streaming.state import IndicatorState  # noqa: E402

LOGGER = logging.getLogger("streaming.job")


@dataclass(frozen=True)
class BarQuery:
    """One streaming query: a bar size, its Cassandra table and checkpoint name."""

    name: str
    window: str
    table: str
    #: Indicators and alerts are derived from 1-minute bars only (z-score vs 30 minutes).
    derive_indicators: bool


BAR_QUERIES = (
    BarQuery("speed-ohlcv-1m", "1 minute", "ohlcv_1m", derive_indicators=True),
    BarQuery("speed-ohlcv-5m", "5 minutes", "ohlcv_5m", derive_indicators=False),
)


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
    parser.add_argument("--trigger", default="5 seconds")
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="process everything available, then stop (file mode, tests)",
    )
    # PDF Kiến trúc, "Watermark": late trades beyond this delay no longer change a bar.
    parser.add_argument("--watermark", default="30 seconds")
    parser.add_argument(
        "--queries",
        default=",".join(query.name for query in BAR_QUERIES),
        help="subset of streaming queries to start",
    )
    parser.add_argument("--output-mode", choices=["cassandra", "parquet"], default="cassandra")
    parser.add_argument("--parquet-path", default="s3a://lake/speed")
    parser.add_argument("--cassandra-host", default=os.getenv("CASSANDRA_HOSTS", "127.0.0.1"))
    parser.add_argument(
        "--cassandra-port", type=int, default=int(os.getenv("CASSANDRA_PORT", "9042"))
    )
    parser.add_argument("--keyspace", default=os.getenv("CASSANDRA_KEYSPACE", "market"))
    parser.add_argument("--alert-topic", default="alerts")
    parser.add_argument("--disable-alert-kafka", action="store_true")
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=int(os.getenv("SPEED_METRICS_PORT", "9108")),
        help="Prometheus /metrics port on the driver, 0 disables it",
    )
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
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(endpoint.startswith("https")).lower(),
            )
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
    for column in ("window_start", "window_end", "last_event_time", "updated_at"):
        if column in pdf.columns:
            # Arrow returns naive timestamps in the session time zone, which is UTC.
            pdf[column] = pd.to_datetime(pdf[column], utc=True)
    return pdf


class SpeedLayer:
    """``foreachBatch`` handlers: upsert bars, then indicators/alerts from 1m bars."""

    def __init__(
        self,
        spark: SparkSession,
        sink: Sink,
        alert_publisher: KafkaAlertPublisher | None = None,
        state: IndicatorState | None = None,
        metrics: SpeedMetrics | None = None,
    ) -> None:
        self.spark = spark
        self.sink = sink
        self.alert_publisher = alert_publisher
        self.state = state or IndicatorState()
        self.metrics = metrics

    def handler(self, query: BarQuery) -> Callable[[DataFrame, int], None]:
        def process(batch_df: DataFrame, batch_id: int) -> None:
            self.process_bars(query, batch_df, batch_id)

        return process

    def process_bars(self, query: BarQuery, batch_df: DataFrame, batch_id: int) -> None:
        # In update mode a batch holds every bar that received trades since the last
        # trigger, partial ones included; the upsert replaces the previous version.
        bars = batch_df.withColumn("updated_at", F.current_timestamp()).select(*OHLCV_COLUMNS)
        bars.persist()
        try:
            self.sink.write(query.table, bars)
            if query.derive_indicators:
                pdf = _to_pandas_utc(bars)
            elif self.metrics is not None:
                pdf = _to_pandas_utc(bars.select("updated_at", "last_event_time"))
            else:
                pdf = pd.DataFrame()
        finally:
            bars.unpersist()
        if self.metrics is not None and not pdf.empty:
            self.metrics.record_latency(query.name, latency_seconds(pdf))
        if pdf.empty or not query.derive_indicators:
            return
        update = self.state.update(pdf, pd.Timestamp.now(tz="UTC"))
        if not update.indicators.empty:
            self.sink.write(
                "indicators_latest",
                self.spark.createDataFrame(update.indicators, INDICATOR_SCHEMA),
            )
        if not update.alerts.empty:
            # Cassandra gets every alert of the batch (upsert by alert_id is harmless);
            # Kafka only the ones this driver has not published yet.
            self.sink.write(
                "alerts_by_day", self.spark.createDataFrame(update.alerts, ALERT_SCHEMA)
            )
        if not update.new_alerts.empty and self.alert_publisher is not None:
            self.alert_publisher.publish(
                self.spark.createDataFrame(update.new_alerts, ALERT_SCHEMA)
            )
        LOGGER.info(
            "query=%s batch=%s bars=%s alerts=%s new_alerts=%s",
            query.name,
            batch_id,
            len(pdf),
            len(update.alerts),
            len(update.new_alerts),
        )


def start_queries(
    spark: SparkSession, args: argparse.Namespace, layer: SpeedLayer
) -> list[StreamingQuery]:
    wanted = {name for name in args.queries.split(",") if name}
    queries: list[StreamingQuery] = []
    for query in BAR_QUERIES:
        if query.name not in wanted:
            continue
        bars = aggregate_ohlcv(trade_stream(spark, args), query.window)
        writer = (
            bars.writeStream.queryName(query.name)
            .outputMode("update")
            .foreachBatch(layer.handler(query))
            .option("checkpointLocation", f"{args.checkpoint_root.rstrip('/')}/{query.name}")
        )
        if args.available_now:
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime=args.trigger)
        queries.append(writer.start())
    if not queries:
        raise SystemExit(f"no known query in --queries={args.queries}")
    return queries


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = build_spark(args)
    spark.sparkContext.setLogLevel("WARN")
    publisher = None
    if not args.disable_alert_kafka and args.source == "kafka":
        publisher = KafkaAlertPublisher(args.bootstrap, args.alert_topic)
    # Without Cassandra (parquet mode) there is nothing to restore state from.
    loader = CassandraBarLoader(spark, args.keyspace) if args.output_mode == "cassandra" else None
    metrics = None
    if args.metrics_port:
        metrics = SpeedMetrics()
        serve(args.metrics_port)
        spark.streams.addListener(make_progress_listener(metrics))
    layer = SpeedLayer(spark, build_sink(args), publisher, IndicatorState(loader=loader), metrics)
    queries = start_queries(spark, args, layer)
    if args.available_now:
        for query in queries:
            query.awaitTermination()
        return
    # A failing query stops the driver; the SparkApplication restartPolicy brings it
    # back and both queries resume from their checkpoints.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
