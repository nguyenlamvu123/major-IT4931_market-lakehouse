"""Read-only Cassandra queries used by the API."""

from __future__ import annotations

import logging
import os
from typing import Any

LOGGER = logging.getLogger(__name__)


class CassandraRepository:
    def __init__(self) -> None:
        self.keyspace = os.getenv("CASSANDRA_KEYSPACE", "market")
        self._session: Any = None
        try:
            from cassandra.cluster import Cluster

            hosts = [
                host.strip()
                for host in os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")
                if host.strip()
            ]
            port = int(os.getenv("CASSANDRA_PORT", "9042"))
            cluster = Cluster(hosts, port=port, connect_timeout=3)
            self._session = cluster.connect(self.keyspace)
        except Exception as exc:  # noqa: BLE001 - API must start without Cassandra
            LOGGER.warning("Cassandra unavailable, using demo data: %s", exc)

    @property
    def available(self) -> bool:
        return self._session is not None

    def bars(self, symbol: str, limit: int) -> list[dict[str, Any]]:
        rows = self._session.execute(
            f"SELECT * FROM {self.keyspace}.ohlcv_1m WHERE symbol = %s LIMIT %s",
            (symbol.upper(), limit),
        )
        return [dict(row._asdict()) for row in rows]

    def alerts(self, symbol: str, limit: int) -> list[dict[str, Any]]:
        rows = self._session.execute(
            f"SELECT * FROM {self.keyspace}.alerts WHERE symbol = %s LIMIT %s",
            (symbol.upper(), limit),
        )
        return [dict(row._asdict()) for row in rows]

    def latest(self, symbol: str) -> dict[str, Any] | None:
        rows = self._session.execute(
            f"SELECT * FROM {self.keyspace}.latest_price WHERE symbol = %s",
            (symbol.upper(),),
        )
        row = rows.one()
        return None if row is None else dict(row._asdict())
