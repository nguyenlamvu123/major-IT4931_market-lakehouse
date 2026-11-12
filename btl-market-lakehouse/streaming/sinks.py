"""Where speed-layer frames go: Cassandra (cluster), Parquet (local debug), Kafka alerts.

Idempotency model: Spark gives at-least-once delivery to ``foreachBatch`` (a micro-batch
is replayed after a crash). Cassandra writes are upserts on the primary key, so a
replayed batch rewrites the same rows instead of adding new ones; that is what makes the
Cassandra output effectively exactly-once. Parquet output is append-only and keeps every
version of a bar (latest ``updated_at`` wins when reading).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .columns import ALERT_COLUMNS, check_columns

LOGGER = logging.getLogger(__name__)


class Sink(Protocol):
    def write(self, table: str, frame: DataFrame) -> None: ...


@dataclass(frozen=True)
class CassandraSettings:
    host: str = "127.0.0.1"
    port: int = 9042
    keyspace: str = "market"
    username: str = ""
    password: str = ""

    def spark_options(self) -> dict[str, str]:
        options = {
            "spark.cassandra.connection.host": self.host,
            "spark.cassandra.connection.port": str(self.port),
        }
        if self.username:
            options["spark.cassandra.auth.username"] = self.username
            options["spark.cassandra.auth.password"] = self.password
        return options


class CassandraSink:
    """Upsert into ``market.<table>`` through the Spark Cassandra Connector."""

    def __init__(self, settings: CassandraSettings) -> None:
        self.settings = settings

    def write(self, table: str, frame: DataFrame) -> None:
        check_columns(table, frame.columns)
        (
            frame.write.format("org.apache.spark.sql.cassandra")
            .options(keyspace=self.settings.keyspace, table=table)
            # "append" in the connector is an upsert by primary key.
            .mode("append")
            .save()
        )


class ParquetSink:
    """Append each table to ``<root>/<table>/`` (local runs without Cassandra)."""

    def __init__(self, root: str) -> None:
        self.root = root.rstrip("/")

    def write(self, table: str, frame: DataFrame) -> None:
        check_columns(table, frame.columns)
        frame.write.mode("append").parquet(f"{self.root}/{table}")


class KafkaAlertPublisher:
    """Publish alerts to the ``alerts`` topic (key = symbol, value = alert.schema JSON)."""

    def __init__(self, bootstrap: str, topic: str = "alerts") -> None:
        self.bootstrap = bootstrap
        self.topic = topic

    @staticmethod
    def to_records(alerts: DataFrame) -> DataFrame:
        """``alerts_by_day`` rows -> Kafka ``key``/``value`` columns (alert.schema.json)."""
        # The JSON contract carries day and alert_time as ISO-8601 strings (UTC session).
        iso_formats = {"day": "yyyy-MM-dd", "alert_time": "yyyy-MM-dd'T'HH:mm:ssXXX"}
        fields = [
            F.date_format(name, iso_formats[name]).alias(name)
            if name in iso_formats
            else F.col(name)
            for name in ALERT_COLUMNS
        ]
        return alerts.select(
            F.col("symbol").alias("key"),
            F.to_json(F.struct(*fields)).alias("value"),
        )

    def publish(self, alerts: DataFrame) -> None:
        (
            self.to_records(alerts)
            .write.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap)
            .option("topic", self.topic)
            .save()
        )


class CassandraBarLoader:
    """Restore a symbol's newest 1m bars from ``ohlcv_1m`` after a driver restart.

    Reads the partitions ``(symbol, as_of day)`` and ``(symbol, previous day)`` so the
    lookback survives midnight UTC; both are partition-key lookups, not table scans.
    """

    def __init__(self, spark: SparkSession, keyspace: str, lookback: int = 60) -> None:
        self.spark = spark
        self.keyspace = keyspace
        self.lookback = lookback

    def __call__(self, symbol: str, as_of: pd.Timestamp) -> pd.DataFrame:
        days = [(as_of - pd.Timedelta(days=offset)).date() for offset in (0, 1)]
        frame = (
            self.spark.read.format("org.apache.spark.sql.cassandra")
            .options(keyspace=self.keyspace, table="ohlcv_1m")
            .load()
            .filter((F.col("symbol") == symbol) & F.col("day").isin(days))
            .orderBy(F.col("window_start").desc())
            .limit(self.lookback)
        )
        LOGGER.info("restoring indicator state for %s from %s", symbol, days)
        return frame.toPandas()
