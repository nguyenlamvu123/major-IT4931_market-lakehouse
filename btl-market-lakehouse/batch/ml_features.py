"""Feature engineering for MLlib asset clustering (pure pandas)."""

from __future__ import annotations

import pandas as pd


def symbol_features(daily: pd.DataFrame) -> pd.DataFrame:
    required = {"symbol", "day", "close", "volume"}
    missing = required.difference(daily.columns)
    if missing:
        raise ValueError(f"symbol_features missing columns: {sorted(missing)}")
    if daily.empty:
        return pd.DataFrame(columns=["symbol", "mean_return", "volatility", "avg_volume"])

    pivot = daily.pivot(index="day", columns="symbol", values="close").sort_index()
    returns = pivot.pct_change(fill_method=None)
    volume = daily.groupby("symbol")["volume"].mean()

    features = pd.DataFrame(
        {
            "symbol": returns.columns,
            "mean_return": returns.mean().values,
            "volatility": returns.std().values,
        }
    )
    features["avg_volume"] = features["symbol"].map(volume).fillna(0.0)
    return features.fillna(0.0).reset_index(drop=True)
