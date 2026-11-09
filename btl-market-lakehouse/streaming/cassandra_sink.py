"""Cassandra sink for OHLCV bars and alerts.

Cassandra writes are upserts by primary key. The keys are
`((symbol), window_start)` for bars and `((symbol), alert_time, alert_id)` for alerts,
so replaying a micro-batch is idempotent.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CassandraSettings:
    host: str = "127.0.0.1"
    port: int = 9042
    keyspace: str = "market"

    def spark_options(self) -> dict[str, str]:
        return {
            "spark.cassandra.connection.host": self.host,
            "spark.cassandra.connection.port": str(self.port),
        }


class CassandraSink:
    def __init__(self, settings: CassandraSettings) -> None:
        self.settings = settings

    def _write(self, frame: DataFrame, table: str) -> None:
        (
            frame.write.format("org.apache.spark.sql.cassandra")
            .options(
                keyspace=self.settings.keyspace,
                table=table,
                **{"spark.cassandra.output.consistency.level": "LOCAL_QUORUM"},
            )
            .mode("append")
            .save()
        )

    def write_bars(self, frame: DataFrame) -> None:
        self._write(frame, "ohlcv_1m")

    def write_alerts(self, frame: DataFrame) -> None:
        self._write(frame, "alerts")
