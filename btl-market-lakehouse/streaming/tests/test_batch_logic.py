"""Unit tests for the pure foreachBatch enrichment (no Spark required)."""

from __future__ import annotations

import pandas as pd

from streaming.batch_logic import alerts_frame, enrich_batch


def _bars() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["AAPL"] * 40,
            "window_start": pd.date_range("2026-01-02T10:00:00Z", periods=40, freq="1min"),
            "open": [100.0 + index for index in range(40)],
            "high": [100.5 + index for index in range(40)],
            "low": [99.5 + index for index in range(40)],
            "close": [100.0 + index for index in range(40)],
            "volume": [1.0 + index % 5 for index in range(40)],
            "vwap": [100.0 + index for index in range(40)],
            "trade_count": [1] * 40,
        }
    )


def test_enrich_batch_returns_bar_columns() -> None:
    enriched, alerts = enrich_batch(_bars())
    assert "sma_5" in enriched.columns
    assert "zscore_30m" in enriched.columns
    assert isinstance(alerts, list)


def test_alerts_frame_converts_time() -> None:
    frame = alerts_frame(
        [
            {
                "alert_id": "a" * 16,
                "symbol": "AAPL",
                "alert_time": "2026-01-02T10:30:00+00:00",
                "alert_type": "PRICE_SPIKE",
                "severity": "medium",
                "price": 200.0,
                "zscore": 4.0,
                "volume_zscore": None,
                "message": "test",
            }
        ]
    )
    assert len(frame) == 1
    assert str(frame.iloc[0]["alert_time"]).startswith("2026-01-02")
