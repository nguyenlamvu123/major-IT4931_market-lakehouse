"""Tests for ML feature engineering."""

from __future__ import annotations

import pandas as pd

from batch.ml_features import symbol_features


def test_symbol_features() -> None:
    daily = pd.DataFrame(
        {
            "symbol": ["AAA"] * 5 + ["BBB"] * 5,
            "day": [f"2026-01-0{index}" for index in range(1, 6)] * 2,
            "close": [10, 11, 12, 13, 14, 20, 19, 21, 20, 22],
            "volume": [100] * 5 + [200] * 5,
        }
    )
    features = symbol_features(daily)
    assert len(features) == 2
    assert set(features.columns) == {"symbol", "mean_return", "volatility", "avg_volume"}
    assert features.loc[features["symbol"] == "BBB", "avg_volume"].iloc[0] == 200.0
