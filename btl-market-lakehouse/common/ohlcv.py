"""OHLCV aggregation helpers shared by the Spark streaming job and the demo."""

from __future__ import annotations

import numpy as np
import pandas as pd


def to_utc_datetime(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_datetime(series, unit="ms", utc=True)
    return pd.to_datetime(series, utc=True)


def aggregate_trades(trades: pd.DataFrame, freq: str = "1min") -> pd.DataFrame:
    """Aggregate raw trades into OHLCV bars per symbol and time window."""
    required = {"symbol", "event_time", "price", "volume"}
    missing = required.difference(trades.columns)
    if missing:
        raise ValueError(f"aggregate_trades missing columns: {sorted(missing)}")
    if trades.empty:
        return pd.DataFrame(
            columns=[
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
        )

    frame = trades.copy()
    frame["event_time"] = to_utc_datetime(frame["event_time"])
    frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "event_time", "price", "volume"])
    frame["notional"] = frame["price"] * frame["volume"]
    frame = frame.set_index("event_time")

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
    return bars.sort_values(["symbol", "window_start"]).reset_index(drop=True)
