"""Gold Parquet -> Cassandra batch tables (contracts/cassandra.cql), read by market-api (D).

| gold table          | Cassandra table          | primary key                                   |
|---------------------|--------------------------|-----------------------------------------------|
| daily_summary       | market.daily_summary     | ((symbol), day)                               |
| correlation_matrix  | market.correlation_matrix| ((window_days), as_of, base_symbol, quote_symbol) |
| yield_curve         | market.yield_curve       | ((curve), day)                                |
| asset_clusters      | market.asset_clusters    | ((model), as_of, symbol)                      |

Cassandra writes are upserts by primary key, so re-running a day overwrites the same
rows instead of duplicating them. Columns are selected and cast from the CQL contract
itself: a column renamed in the contract fails :func:`prepare` loudly instead of
silently writing NULLs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from common.cql import load_tables

#: Gold tables written by this sink (gold name == Cassandra table name).
SINK_TABLES = ("daily_summary", "correlation_matrix", "yield_curve", "asset_clusters")

CQL_TO_SPARK: dict[str, DataType] = {
    "text": StringType(),
    "date": DateType(),
    "timestamp": TimestampType(),
    "int": IntegerType(),
    "bigint": LongType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
}


@dataclass(frozen=True)
class CassandraSettings:
    """Connection settings; defaults follow contracts/infra.yaml and the env names there."""

    hosts: str = "127.0.0.1"
    port: int = 9042
    keyspace: str = "market"
    username: str = ""
    password: str = ""

    @classmethod
    def from_env(cls) -> CassandraSettings:
        return cls(
            hosts=os.getenv("CASSANDRA_HOSTS", cls.hosts),
            port=int(os.getenv("CASSANDRA_PORT", str(cls.port))),
            keyspace=os.getenv("CASSANDRA_KEYSPACE", cls.keyspace),
            username=os.getenv("CASSANDRA_USERNAME", ""),
            password=os.getenv("CASSANDRA_PASSWORD", ""),
        )

    def spark_conf(self) -> dict[str, str]:
        conf = {
            "spark.cassandra.connection.host": self.hosts,
            "spark.cassandra.connection.port": str(self.port),
        }
        # K8ssandra enables authentication by default; the dev compose node does not.
        if self.username:
            conf["spark.cassandra.auth.username"] = self.username
            conf["spark.cassandra.auth.password"] = self.password
        return conf


def prepare(frame: DataFrame, table: str) -> DataFrame:
    """Select exactly the contract columns of ``table``, cast to their CQL types.

    Extra gold-only columns (``dt``, ``price_source``) are dropped; a contract column
    missing from the frame raises ``ValueError``.
    """
    contract = load_tables()[table]
    missing = [column for column in contract.columns if column not in frame.columns]
    if missing:
        raise ValueError(f"gold frame for {table} lacks contract columns {missing}")
    return frame.select(
        [
            F.col(column).cast(CQL_TO_SPARK[cql_type]).alias(column)
            for column, cql_type in contract.columns.items()
        ]
    )


def write(frame: DataFrame, table: str, settings: CassandraSettings) -> None:
    """Append (= upsert) the prepared rows through the Spark Cassandra Connector.

    Needs the ``spark-cassandra-connector`` jar on the classpath (baked into
    ``btl/batch:dev``) and ``settings.spark_conf()`` applied to the session.
    """
    (
        prepare(frame, table)
        .write.format("org.apache.spark.sql.cassandra")
        .options(keyspace=settings.keyspace, table=table)
        .option("spark.cassandra.output.consistency.level", "LOCAL_QUORUM")
        .mode("append")
        .save()
    )
