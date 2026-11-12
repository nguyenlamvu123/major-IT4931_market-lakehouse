"""OHLCV aggregation helpers shared by the streaming layer, batch layer and the demo."""

from __future__ import annotations

import numpy as np
import pandas as pd

BAR_COLUMNS = [
    "symbol",
    "window_start",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trade_count",
]


def to_utc_datetime(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_datetime(series, unit="ms", utc=True)
    return pd.to_datetime(series, utc=True)


def dedupe_trades(trades: pd.DataFrame) -> pd.DataFrame:
    """Drop re-sent trades (producer reconnects) using the contract ``trade_id``.

    Records without a ``trade_id`` column are left untouched: the contract makes the id
    mandatory, so a missing column means the caller passed hand-made test data.
    """
    if "trade_id" not in trades.columns:
        return trades
    return trades.drop_duplicates(subset=["trade_id"], keep="first")


def aggregate_trades(trades: pd.DataFrame, freq: str = "1min") -> pd.DataFrame:
    """Aggregate raw trades into OHLCV bars per symbol and time window.

    VWAP is ``sum(price*volume) / sum(volume)``; it is left empty (NaN) for windows with
    zero volume, e.g. the dollar index whose Yahoo feed has no volume.
    """
    required = {"symbol", "event_time", "price", "volume"}
    missing = required.difference(trades.columns)
    if missing:
        raise ValueError(f"aggregate_trades missing columns: {sorted(missing)}")
    keep_asset_class = "asset_class" in trades.columns
    columns = BAR_COLUMNS + (["asset_class"] if keep_asset_class else [])
    if trades.empty:
        return pd.DataFrame(columns=columns)

    frame = dedupe_trades(trades).copy()
    frame["event_time"] = to_utc_datetime(frame["event_time"])
    frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "event_time", "price", "volume"])
    frame["notional"] = frame["price"] * frame["volume"]
    # Ties on event_time keep arrival order, which is the order the feed reported them.
    frame = frame.sort_values(["symbol", "event_time"], kind="stable").set_index("event_time")

    bars = (
        frame.groupby("symbol")[["price", "volume", "notional"]]
        .resample(freq)
        .agg(
            open=("price", "first"),
            high=("price", "max"),
            low=("price", "min"),
            close=("price", "last"),
            volume=("volume", "sum"),
            notional=("notional", "sum"),
            trade_count=("price", "size"),
        )
        .reset_index()
        .rename(columns={"event_time": "window_start"})
    )
    bars = bars[bars["trade_count"] > 0].copy()
    bars["vwap"] = bars["notional"] / bars["volume"].replace(0, np.nan)
    bars = bars.drop(columns=["notional"])
    if keep_asset_class:
        classes = frame.groupby("symbol")["asset_class"].first()
        bars["asset_class"] = bars["symbol"].map(classes)
    return bars[columns].sort_values(["symbol", "window_start"]).reset_index(drop=True)
