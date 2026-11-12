"""Cassandra-backed repository (keyspace ``market``, tables of contracts/cassandra.cql).

Connection state machine:

- ``_session is None``: not connected. The next call tries to connect, at most once every
  ``RETRY_SECONDS`` so a dead Cassandra is not hammered by every Grafana panel refresh.
- connected: queries run on the shared session. Any driver error drops the session and
  raises :class:`RepositoryUnavailable`; the API then decides between demo data
  (``DEMO_FALLBACK=true``) and HTTP 503. It never returns an empty list for a failure,
  so "no data yet" and "database down" stay distinguishable.

Connecting is lazy: importing the API (tests, uvicorn start) never touches the network.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

from common.io_utils import jsonable

from .queries import QUERIES
from .repository import RepositoryUnavailable, Row

LOGGER = logging.getLogger(__name__)

YIELD_CURVE_NAME = "UST"
CLUSTER_MODEL = "kmeans"
_JSON_TYPES = (str, int, float, bool, list, dict, type(None))


def _plain(value: Any) -> Any:
    """Driver value -> JSON value.

    Before:
    - datetime(2026, 11, 9, 10, 0)            (driver timestamps are naive UTC)
    - cassandra.util.Date(2026-11-09)

    After:
    - "2026-11-09T10:00:00+00:00"
    - "2026-11-09"
    """
    if isinstance(value, datetime) and value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = jsonable(value)
    return value if isinstance(value, _JSON_TYPES) else str(value)


class CassandraRepository:
    source = "cassandra"
    RETRY_SECONDS = 5.0

    def __init__(self, session: Any | None = None) -> None:
        self.hosts = [
            host.strip()
            for host in os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")
            if host.strip()
        ]
        self.port = int(os.getenv("CASSANDRA_PORT", "9042"))
        # An injected session (tests) is never dropped or reconnected.
        self._injected = session is not None
        self._session: Any = session
        self._last_attempt = float("-inf")
        self._lock = threading.Lock()

    def _connect(self) -> Any:
        with self._lock:
            if self._session is not None:
                return self._session
            now = time.monotonic()
            if now - self._last_attempt < self.RETRY_SECONDS:
                raise RepositoryUnavailable("cassandra unavailable (retry pending)")
            self._last_attempt = now
            try:
                from cassandra.auth import PlainTextAuthProvider
                from cassandra.cluster import Cluster
                from cassandra.query import dict_factory

                username = os.getenv("CASSANDRA_USERNAME")
                auth = (
                    PlainTextAuthProvider(username, os.getenv("CASSANDRA_PASSWORD", ""))
                    if username
                    else None
                )
                cluster = Cluster(self.hosts, port=self.port, connect_timeout=3, auth_provider=auth)
                session = cluster.connect()
                session.row_factory = dict_factory
                session.default_timeout = 5
            except Exception as exc:  # noqa: BLE001 - any driver/network error means "down"
                LOGGER.warning("Cassandra unavailable: %s", exc)
                raise RepositoryUnavailable(f"cassandra unavailable: {exc}") from exc
            self._session = session
            return session

    @property
    def available(self) -> bool:
        try:
            self._connect()
        except RepositoryUnavailable:
            return False
        return True

    def _rows(self, name: str, *params: Any) -> list[Row]:
        session = self._connect()
        query = QUERIES[name]
        try:
            result = session.execute(query.cql, params)
        except Exception as exc:  # noqa: BLE001 - reconnect on the next request
            LOGGER.warning("Cassandra query %s failed, dropping session: %s", name, exc)
            if not self._injected:
                self._session = None
            raise RepositoryUnavailable(f"query {name} failed: {exc}") from exc
        return [{key: _plain(value) for key, value in dict(row).items()} for row in result]

    def default_day(self) -> str:
        # Realtime tables are partitioned by UTC day; "today" is the hot partition.
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def ohlcv(self, symbol: str, day: str, interval: str, limit: int) -> list[Row]:
        return self._rows(f"ohlcv_{interval}", symbol, day, limit)

    def indicators(self, symbol: str | None) -> list[Row]:
        if symbol:
            return self._rows("indicators_one", symbol)
        return self._rows("indicators_all")

    def alerts(self, day: str, limit: int) -> list[Row]:
        return self._rows("alerts_day", day, limit)

    def daily(self, symbol: str, limit: int) -> list[Row]:
        return self._rows("daily_symbol", symbol, limit)

    def correlation(self, window_days: int) -> list[Row]:
        latest = self._rows("correlation_latest_as_of", window_days, 1)
        if not latest:
            return []
        return self._rows("correlation_rows", window_days, latest[0]["as_of"])

    def yield_curve(self, limit: int) -> list[Row]:
        return self._rows("yield_curve", YIELD_CURVE_NAME, limit)

    def clusters(self) -> list[Row]:
        latest = self._rows("clusters_latest_as_of", CLUSTER_MODEL, 1)
        if not latest:
            return []
        return self._rows("clusters_rows", CLUSTER_MODEL, latest[0]["as_of"])
