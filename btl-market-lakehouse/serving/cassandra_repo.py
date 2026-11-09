"""Read-only Cassandra queries used by the API."""

from __future__ import annotations

import logging
import os
import time
from typing import Any

LOGGER = logging.getLogger(__name__)


class CassandraRepository:
    RETRY_SECONDS = 5.0

    def __init__(self) -> None:
        self.keyspace = os.getenv("CASSANDRA_KEYSPACE", "market")
        self._session: Any = None
        self._last_attempt = 0.0
        self._maybe_connect()

    def _maybe_connect(self) -> None:
        if self._session is not None:
            return
        now = time.monotonic()
        if now - self._last_attempt < self.RETRY_SECONDS:
            return
        self._last_attempt = now
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

    def _execute(self, query: str, params: tuple[Any, ...]) -> Any:
        self._maybe_connect()
        if self._session is None:
            return None
        try:
            return self._session.execute(query, params)
        except Exception as exc:  # noqa: BLE001 - reconnect on the next request
            LOGGER.warning("Cassandra query failed, dropping session: %s", exc)
            self._session = None
            return None

    @property
    def available(self) -> bool:
        self._maybe_connect()
        return self._session is not None

    def bars(self, symbol: str, limit: int) -> list[dict[str, Any]]:
        rows = self._execute(
            f"SELECT * FROM {self.keyspace}.ohlcv_1m WHERE symbol = %s LIMIT %s",
            (symbol.upper(), limit),
        )
        if rows is None:
            return []
        return [dict(row._asdict()) for row in rows]

    def alerts(self, symbol: str, limit: int) -> list[dict[str, Any]]:
        rows = self._execute(
            f"SELECT * FROM {self.keyspace}.alerts WHERE symbol = %s LIMIT %s",
            (symbol.upper(), limit),
        )
        if rows is None:
            return []
        return [dict(row._asdict()) for row in rows]

    def latest(self, symbol: str) -> dict[str, Any] | None:
        rows = self._execute(
            f"SELECT * FROM {self.keyspace}.latest_price WHERE symbol = %s",
            (symbol.upper(),),
        )
        if rows is None:
            return None
        row = rows.one()
        return None if row is None else dict(row._asdict())
