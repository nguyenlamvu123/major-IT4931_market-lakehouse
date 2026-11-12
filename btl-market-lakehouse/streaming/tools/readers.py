"""Read speed-layer output back for the operator tools (Cassandra or local Parquet).

Both readers return the *current* row per primary key, i.e. what a dashboard sees:
Cassandra already upserts, while the Parquet sink appends every version of a bar, so
the newest ``updated_at`` per ``(symbol, window_start)`` is kept.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

BAR_KEY = ["symbol", "window_start"]


def latest_per_key(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    frame = frame.assign(window_start=pd.to_datetime(frame["window_start"], utc=True))
    return (
        frame.sort_values("updated_at", kind="stable")
        .groupby(BAR_KEY, as_index=False)
        .tail(1)
        .sort_values(BAR_KEY)
        .reset_index(drop=True)
    )


def read_parquet_table(root: str, table: str) -> pd.DataFrame:
    path = Path(root) / table
    if not path.exists():
        return pd.DataFrame()
    return latest_per_key(pd.read_parquet(path))


def read_cassandra_table(
    hosts: list[str],
    port: int,
    keyspace: str,
    table: str,
    symbols: list[str],
    days: list[str],
) -> pd.DataFrame:
    """Read ``ohlcv_1m``/``ohlcv_5m`` partitions ``(symbol, day)`` with cassandra-driver."""
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    auth = None
    if os.getenv("CASSANDRA_USERNAME"):
        auth = PlainTextAuthProvider(
            os.environ["CASSANDRA_USERNAME"], os.getenv("CASSANDRA_PASSWORD", "")
        )
    cluster = Cluster(hosts, port=port, auth_provider=auth)
    try:
        session = cluster.connect(keyspace)
        rows: list[dict] = []
        query = session.prepare(f"SELECT * FROM {table} WHERE symbol = ? AND day = ?")
        for symbol in symbols:
            for day in days:
                for row in session.execute(query, (symbol, pd.Timestamp(day).date())):
                    rows.append(dict(row._asdict()))
    finally:
        cluster.shutdown()
    return latest_per_key(pd.DataFrame(rows))
