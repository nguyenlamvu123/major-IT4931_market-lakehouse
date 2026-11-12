"""Columns the speed layer writes to Cassandra (no pyspark import, unit-testable).

Every list here must be a subset of the matching table in ``contracts/cassandra.cql``
and contain its full primary key; ``streaming/tests/test_contract_columns.py`` enforces
that with ``common.cql.load_tables()``. The sink also checks it at runtime so a drifted
frame fails before the Spark Cassandra Connector rejects the write on the cluster.
"""

from __future__ import annotations

#: ``ohlcv_1m`` and ``ohlcv_5m`` share one shape (PK ``((symbol, day), window_start)``).
OHLCV_COLUMNS = [
    "symbol",
    "day",
    "window_start",
    "window_end",
    "asset_class",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trade_count",
    "last_event_time",
    "updated_at",
]

#: One row per symbol: the indicators of its newest 1-minute bar.
INDICATOR_COLUMNS = [
    "symbol",
    "asset_class",
    "window_start",
    "close",
    "vwap",
    "return_1m",
    "sma_5",
    "sma_15",
    "ema_12",
    "ema_26",
    "macd",
    "macd_signal",
    "macd_hist",
    "rsi_14",
    "volatility_30m",
    "zscore_30m",
    "volume_zscore_30m",
    "updated_at",
]

#: ``alerts_by_day`` row; ``day`` is the partition key ("cảnh báo hôm nay").
ALERT_COLUMNS = [
    "day",
    "alert_time",
    "symbol",
    "alert_id",
    "asset_class",
    "alert_type",
    "severity",
    "price",
    "zscore",
    "volume_zscore",
    "message",
]

#: Cassandra table -> exact column list the speed layer writes into it.
CASSANDRA_WRITES: dict[str, list[str]] = {
    "ohlcv_1m": OHLCV_COLUMNS,
    "ohlcv_5m": OHLCV_COLUMNS,
    "indicators_latest": INDICATOR_COLUMNS,
    "alerts_by_day": ALERT_COLUMNS,
}


def check_columns(table: str, columns: list[str]) -> None:
    """Raise when a frame about to be written to ``table`` does not match the contract.

    Order does not matter; extra or missing columns do. Called by every sink write.
    """
    expected = CASSANDRA_WRITES.get(table)
    if expected is None:
        raise ValueError(f"speed layer does not write table {table!r}")
    extra = sorted(set(columns) - set(expected))
    missing = sorted(set(expected) - set(columns))
    if extra or missing:
        raise ValueError(f"{table}: columns differ from contract, extra={extra} missing={missing}")
