"""Daily, correlation, yield-curve and ML feature helpers for the batch layer."""

from __future__ import annotations

import numpy as np
import pandas as pd


def daily_summary(bars: pd.DataFrame) -> pd.DataFrame:
    required = {"symbol", "window_start", "open", "high", "low", "close", "volume", "vwap"}
    missing = required.difference(bars.columns)
    if missing:
        raise ValueError(f"daily_summary missing columns: {sorted(missing)}")
    if bars.empty:
        return pd.DataFrame(
            columns=["symbol", "day", "open", "high", "low", "close", "volume", "vwap"]
        )

    frame = bars.copy()
    frame["day"] = pd.to_datetime(frame["window_start"], utc=True).dt.strftime("%Y-%m-%d")
    daily = (
        frame.groupby(["symbol", "day"], as_index=False)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            vwap=("vwap", "last"),
        )
        .sort_values(["symbol", "day"])
    )
    return daily.reset_index(drop=True)


def correlation_matrix(daily: pd.DataFrame, window_days: int = 90) -> pd.DataFrame:
    required = {"symbol", "day", "close"}
    missing = required.difference(daily.columns)
    if missing:
        raise ValueError(f"correlation_matrix missing columns: {sorted(missing)}")
    if daily.empty:
        return pd.DataFrame(
            columns=["base_symbol", "quote_symbol", "window_days", "correlation"]
        )

    pivot = daily.pivot(index="day", columns="symbol", values="close").sort_index()
    returns = pivot.pct_change().dropna(how="all")
    if len(returns) < 2 or len(pivot.columns) < 2:
        return pd.DataFrame(
            columns=["base_symbol", "quote_symbol", "window_days", "correlation"]
        )

    window = returns.tail(window_days)
    corr = window.corr()
    records: list[dict[str, object]] = []
    for base in corr.index:
        for quote in corr.columns:
            if str(base) >= str(quote):
                continue
            value = corr.loc[base, quote]
            if pd.isna(value):
                continue
            records.append(
                {
                    "base_symbol": base,
                    "quote_symbol": quote,
                    "window_days": int(len(window)),
                    "correlation": round(float(value), 6),
                }
            )
    return pd.DataFrame.from_records(records)


def yield_curve(rates: pd.DataFrame) -> pd.DataFrame:
    required = {"day", "tenor", "yield"}
    missing = required.difference(rates.columns)
    if missing:
        raise ValueError(f"yield_curve missing columns: {sorted(missing)}")
    if rates.empty:
        return pd.DataFrame(columns=["day", "2Y", "5Y", "10Y", "30Y", "spread_10y_2y"])

    pivot = (
        rates.pivot_table(index="day", columns="tenor", values="yield", aggfunc="last")
        .sort_index()
        .reset_index()
    )
    for tenor in ("2Y", "5Y", "10Y", "30Y"):
        if tenor not in pivot.columns:
            pivot[tenor] = np.nan
    pivot["spread_10y_2y"] = pivot["10Y"] - pivot["2Y"]
    return pivot.reset_index(drop=True)


def build_ml_features(daily: pd.DataFrame, windows: tuple[int, ...] = (5, 20)) -> pd.DataFrame:
    """Build a simple symbol/day return + rolling-volatility feature table."""
    required = {"symbol", "day", "close"}
    missing = required.difference(daily.columns)
    if missing:
        raise ValueError(f"build_ml_features missing columns: {sorted(missing)}")
    if daily.empty:
        return pd.DataFrame()

    pivot = daily.pivot(index="day", columns="symbol", values="close").sort_index()
    returns = pivot.pct_change()
    output = pd.DataFrame(index=returns.index)
    for window in windows:
        output[f"ret_mean_{window}d"] = returns.rolling(window, min_periods=2).mean().mean(axis=1)
        output[f"ret_std_{window}d"] = returns.rolling(window, min_periods=2).std().mean(axis=1)
    output["breadth"] = (returns > 0).sum(axis=1) / returns.notna().sum(axis=1).replace(0, np.nan)
    return output.dropna(how="all").reset_index()
