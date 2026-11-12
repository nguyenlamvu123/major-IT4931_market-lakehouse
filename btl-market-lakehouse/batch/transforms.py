"""Pandas mirror of the silver trade rules, for the laptop run without Spark.

``batch/run_local_batch.py`` uses it to show the medallion flow on a machine without
Java. The Spark implementation that runs on the cluster is ``batch.silver``; both apply
the same contract rules (contracts/schemas/trade.schema.json).
"""

from __future__ import annotations

import pandas as pd

from common.contracts import ASSET_CLASSES, SYMBOL_PATTERN

_TRADE_ASSET_CLASSES = [name for name in ASSET_CLASSES if name != "rates"]


def clean_trades(trades: pd.DataFrame) -> pd.DataFrame:
    """Typed, validated, de-duplicated trades with a UTC ``dt`` column.

    Before:
    - two rows with trade_id "a" (re-sent), one row with price -1
    - event_time as epoch milliseconds

    After:
    - one row for "a" (the first copy), the price -1 row dropped
    - event_time as UTC timestamp, event_time_ms kept, dt = "YYYY-MM-DD"
    """
    required = {"trade_id", "symbol", "asset_class", "event_time", "price", "volume", "source"}
    missing = required.difference(trades.columns)
    if missing:
        raise ValueError(f"clean_trades missing columns: {sorted(missing)}")
    if trades.empty:
        return trades.assign(event_time_ms=pd.Series(dtype="int64"), dt=pd.Series(dtype=str))

    frame = trades.copy()
    frame["event_time_ms"] = pd.to_numeric(frame["event_time"], errors="coerce")
    frame["event_time"] = pd.to_datetime(frame["event_time_ms"], unit="ms", utc=True)
    frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    valid = (
        frame["trade_id"].notna()
        & frame["symbol"].astype(str).str.fullmatch(SYMBOL_PATTERN.pattern)
        & frame["asset_class"].isin(_TRADE_ASSET_CLASSES)
        & frame["event_time"].notna()
        & (frame["price"] > 0)
        & (frame["volume"] >= 0)
    )
    frame = frame[valid].drop_duplicates(subset=["trade_id"], keep="first")
    frame["dt"] = frame["event_time"].dt.strftime("%Y-%m-%d")
    return frame.sort_values(["symbol", "event_time"]).reset_index(drop=True)
