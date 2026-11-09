"""Tests for pure medallion transforms."""

from __future__ import annotations

import pandas as pd

from batch.transforms import build_correlations, build_curve, clean_trades, daily_bars


def test_clean_trades_deduplicates_and_filters() -> None:
    trades = pd.DataFrame(
        [
            {
                "trade_id": "same",
                "symbol": "AAPL",
                "asset_class": "equity",
                "event_time": 1767364200000,
                "price": 190.0,
                "volume": 2.0,
                "source": "test",
            },
            {
                "trade_id": "same",
                "symbol": "AAPL",
                "asset_class": "equity",
                "event_time": 1767364260000,
                "price": 191.0,
                "volume": 3.0,
                "source": "test",
            },
            {
                "trade_id": "bad",
                "symbol": "AAPL",
                "asset_class": "equity",
                "event_time": 1767364320000,
                "price": -1.0,
                "volume": 1.0,
                "source": "test",
            },
        ]
    )
    result = clean_trades(trades)
    assert len(result) == 1
    assert result.iloc[0]["price"] == 191.0
    assert "event_date" in result.columns


def _bars() -> pd.DataFrame:
    rows = []
    for symbol, offset in (("AAA", 0.0), ("BBB", 10.0)):
        for day in range(1, 6):
            for minute in range(2):
                rows.append(
                    {
                        "symbol": symbol,
                        "window_start": pd.Timestamp(f"2026-01-0{day}T10:0{minute}:00Z"),
                        "open": 100.0 + offset + day,
                        "high": 101.0 + offset + day,
                        "low": 99.0 + offset + day,
                        "close": 100.5 + offset + day,
                        "volume": 10.0 + day,
                        "vwap": 100.2 + offset + day,
                    }
                )
    return pd.DataFrame(rows)


def test_daily_correlation_and_curve() -> None:
    daily = daily_bars(_bars())
    assert len(daily) == 10
    correlations = build_correlations(daily)
    assert len(correlations) == 1

    rates = pd.DataFrame(
        [
            {"day": "2026-01-02", "tenor": "2Y", "yield": 4.0},
            {"day": "2026-01-02", "tenor": "10Y", "yield": 4.5},
        ]
    )
    curve = build_curve(rates)
    assert round(float(curve.iloc[0]["spread_10y_2y"]), 4) == 0.5
