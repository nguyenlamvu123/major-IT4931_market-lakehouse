#!/usr/bin/env python3
"""Speed layer: Kafka -> 1m OHLCV -> indicators/alerts -> Cassandra/Parquet."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from streaming.alerts import alerts_to_dataframe  # noqa: E402
from streaming.cassandra_sink import CassandraSettings, CassandraSink  # noqa: E402
from streaming.indicators import enrich_with_indicators  # noqa: E402
from streaming.ohlcv import aggregate_ohlcv, parse_trades, read_trades  # noqa: E402

LOGGER = logging.getLogger("streaming.job")

DEFAULT_TOPICS = "crypto.trades,equity.trades,commodity.trades,fx.trades"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Market speed layer")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topics", default=DEFAULT_TOPICS)
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--checkpoint", default="s3a://checkpoints/speed")
    parser.add_argument("--trigger", default="30 seconds")
    parser.add_argument("--watermark", default="2 minutes")
    parser.add_argument("--window", default="1 minute")
    parser.add_argument("--output-mode", choices=["cassandra", "parquet"], default="cassandra")
    parser.add_argument("--parquet-path", default="s3a://lake/speed/bars")
    parser.add_argument("--cassandra-host", default="127.0.0.1")
    parser.add_argument("--cassandra-port", type=int, default=9042)
    parser.add_argument("--keyspace", default="market")
    parser.add_argument("--alert-topic", default="market.alerts")
    parser.add_argument("--disable-alert-kafka", action="store_true")
    return parser.parse_args()


def build_spark(args: argparse.Namespace) -> SparkSession:
    cassandra = CassandraSettings(
        host=args.cassandra_host,
        port=args.cassandra_port,
        keyspace=args.keyspace,
    )
    builder = (
        SparkSession.builder.appName("market-speed-layer")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )
    for key, value in cassandra.spark_options().items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


class Pipeline:
    def __init__(self, spark: SparkSession, args: argparse.Namespace) -> None:
        self.spark = spark
        self.args = args
        self.sink = CassandraSink(
            CassandraSettings(
                host=args.cassandra_host,
                port=args.cassandra_port,
                keyspace=args.keyspace,
            )
        )

    def _write_alerts_to_kafka(self, alerts_df: DataFrame) -> None:
        (
            alerts_df.selectExpr(
                "CAST(alert_id AS STRING) AS key",
                "to_json(struct(*)) AS value",
            )
            .write.format("kafka")
            .option("kafka.bootstrap.servers", self.args.bootstrap)
            .option("topic", self.args.alert_topic)
            .save()
        )

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        bars_df, alerts = enrich_with_indicators(batch_df)
        bars_count = 0 if bars_df is None else bars_df.count()
        if bars_df is not None:
            if self.args.output_mode == "cassandra":
                self.sink.write_bars(bars_df)
            else:
                bars_df.write.mode("append").parquet(self.args.parquet_path)

        alerts_df = alerts_to_dataframe(self.spark, alerts)
        if alerts_df is not None:
            if self.args.output_mode == "cassandra":
                self.sink.write_alerts(alerts_df)
            else:
                alerts_df.write.mode("append").parquet(f"{self.args.parquet_path}_alerts")
            if not self.args.disable_alert_kafka:
                self._write_alerts_to_kafka(alerts_df)

        LOGGER.info(
            "batch=%s bars=%s alerts=%s",
            batch_id,
            bars_count,
            len(alerts),
        )


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = build_spark(args)
    spark.sparkContext.setLogLevel("WARN")

    trades = parse_trades(
        read_trades(
            spark,
            bootstrap=args.bootstrap,
            topics=[topic for topic in args.topics.split(",") if topic],
            starting_offsets=args.starting_offsets,
        )
    )
    bars = aggregate_ohlcv(trades, watermark=args.watermark, window_duration=args.window)
    pipeline = Pipeline(spark, args)

    query = (
        bars.writeStream.outputMode("append")
        .foreachBatch(pipeline.process_batch)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
