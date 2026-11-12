"""Unit tests for the pure pandas shaping of indicators and alerts (no Spark)."""

from __future__ import annotations

import datetime as dt

import pandas as pd

from streaming.batch_logic import alert_rows, enrich_batch, indicator_rows
from streaming.columns import ALERT_COLUMNS, INDICATOR_COLUMNS

NOW = pd.Timestamp("2026-11-09T10:45:00Z")


def _bars(count: int = 40) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["AAPL"] * count,
            "asset_class": ["equity"] * count,
            "window_start": pd.date_range("2026-11-09T10:00:00Z", periods=count, freq="1min"),
            "open": [100.0 + index for index in range(count)],
            "high": [100.5 + index for index in range(count)],
            "low": [99.5 + index for index in range(count)],
            "close": [100.0 + index for index in range(count)],
            "volume": [1.0 + index % 5 for index in range(count)],
            "vwap": [100.0 + index for index in range(count)],
            "trade_count": [1] * count,
        }
    )


def test_enrich_batch_returns_contract_frames() -> None:
    indicators, alerts = enrich_batch(_bars(), NOW)
    assert list(indicators.columns) == INDICATOR_COLUMNS
    assert list(alerts.columns) == ALERT_COLUMNS
    assert len(indicators) == 1
    latest = indicators.iloc[0]
    assert latest["window_start"] == pd.Timestamp("2026-11-09T10:39:00Z")
    assert latest["updated_at"] == NOW


def test_indicator_rows_keep_newest_bar_per_symbol() -> None:
    bars = pd.concat([_bars(3), _bars(2).assign(symbol="MSFT")], ignore_index=True)
    rows = indicator_rows(bars, NOW)
    assert sorted(rows["symbol"]) == ["AAPL", "MSFT"]
    assert rows.set_index("symbol").loc["AAPL", "close"] == 102.0


def test_alert_rows_type_day_and_time() -> None:
    frame = alert_rows(
        [
            {
                "alert_id": "a" * 16,
                "symbol": "AAPL",
                "asset_class": "equity",
                "alert_time": "2026-11-09T10:30:00+00:00",
                "day": "2026-11-09",
                "alert_type": "PRICE_SPIKE",
                "severity": "medium",
                "price": 200.0,
                "zscore": 4.0,
                "volume_zscore": None,
                "message": "test",
            }
        ]
    )
    assert list(frame.columns) == ALERT_COLUMNS
    assert frame.iloc[0]["day"] == dt.date(2026, 11, 9)
    assert frame.iloc[0]["alert_time"] == pd.Timestamp("2026-11-09T10:30:00Z")
