"""Pure pandas transforms for the medallion pipeline (unit-testable, no Spark)."""

from __future__ import annotations

import pandas as pd

from common.analytics import (
    build_ml_features,
    correlation_matrix,
    daily_summary,
    yield_curve,
)


def _to_utc(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_datetime(series, unit="ms", utc=True, errors="coerce")
    return pd.to_datetime(series, utc=True, errors="coerce")


def clean_trades(trades: pd.DataFrame) -> pd.DataFrame:
    required = {"symbol", "asset_class", "event_time", "price", "volume", "source"}
    missing = required.difference(trades.columns)
    if missing:
        raise ValueError(f"clean_trades missing columns: {sorted(missing)}")
    if trades.empty:
        return trades.copy()

    frame = trades.copy()
    frame["event_time"] = _to_utc(frame["event_time"])
    frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "event_time", "price", "volume"])
    frame = frame[(frame["price"] > 0) & (frame["volume"] >= 0)].copy()

    if "trade_id" in frame.columns:
        frame = frame.drop_duplicates(subset=["trade_id"], keep="last")
    else:
        frame = frame.drop_duplicates(
            subset=["symbol", "event_time", "price", "volume"], keep="last"
        )

    frame["event_date"] = frame["event_time"].dt.strftime("%Y-%m-%d")
    frame["event_hour"] = frame["event_time"].dt.hour
    return frame.sort_values(["symbol", "event_time"]).reset_index(drop=True)


def daily_bars(bars: pd.DataFrame) -> pd.DataFrame:
    return daily_summary(bars)


def build_correlations(daily: pd.DataFrame, window_days: int = 90) -> pd.DataFrame:
    return correlation_matrix(daily, window_days=window_days)


def build_curve(rates: pd.DataFrame) -> pd.DataFrame:
    return yield_curve(rates)


def build_features(daily: pd.DataFrame) -> pd.DataFrame:
    return build_ml_features(daily)
