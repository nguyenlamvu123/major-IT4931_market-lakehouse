"""Tests for the shared pure-Python pipeline logic."""

from __future__ import annotations

import pandas as pd

from common.alerts import detect_alerts
from common.analytics import correlation_matrix, daily_summary, yield_curve
from common.contracts import make_trade, synthetic_trades, validate_trade
from common.indicators import add_indicators
from common.ohlcv import aggregate_trades


def _ms(value: str) -> int:
    return int(pd.Timestamp(value).timestamp() * 1000)


def test_aggregate_trades_ohlcv_and_vwap() -> None:
    trades = pd.DataFrame(
        [
            {"symbol": "BTCUSDT", "event_time": _ms("2026-01-02T10:00:00Z"), "price": 100.0, "volume": 1.0},
            {"symbol": "BTCUSDT", "event_time": _ms("2026-01-02T10:00:30Z"), "price": 101.0, "volume": 2.0},
            {"symbol": "BTCUSDT", "event_time": _ms("2026-01-02T10:01:10Z"), "price": 99.0, "volume": 3.0},
        ]
    )
    bars = aggregate_trades(trades)
    assert len(bars) == 2
    first = bars.iloc[0]
    assert first["open"] == 100.0
    assert first["high"] == 101.0
    assert first["low"] == 100.0
    assert first["close"] == 101.0
    assert first["volume"] == 3.0
    assert round(float(first["vwap"]), 4) == round((100 + 202) / 3, 4)


def test_indicators_add_expected_columns() -> None:
    bars = pd.DataFrame(
        {
            "symbol": ["AAPL"] * 40,
            "window_start": pd.date_range("2026-01-02T10:00:00Z", periods=40, freq="1min"),
            "open": [100.0 + index for index in range(40)],
            "high": [100.5 + index for index in range(40)],
            "low": [99.5 + index for index in range(40)],
            "close": [100.0 + index for index in range(40)],
            "volume": [1.0 + index % 5 for index in range(40)],
        }
    )
    result = add_indicators(bars)
    for column in ("sma_5", "sma_15", "ema_12", "ema_26", "macd", "rsi_14", "zscore_30m"):
        assert column in result.columns
    assert result["rsi_14"].dropna().between(0, 100).all()


def test_detect_alerts_for_price_and_volume_spikes() -> None:
    bars = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "window_start": pd.Timestamp("2026-01-02T10:00:00Z"),
                "close": 200.0,
                "zscore_30m": 4.5,
                "volume_zscore_30m": 4.0,
            }
        ]
    )
    alerts = detect_alerts(bars, z_threshold=3.0, volume_z_threshold=3.0)
    assert {alert["alert_type"] for alert in alerts} == {"PRICE_SPIKE", "VOLUME_SPIKE"}
    assert all(alert["symbol"] == "AAPL" for alert in alerts)


def test_correlation_and_yield_curve() -> None:
    daily = pd.DataFrame(
        {
            "symbol": ["AAA"] * 5 + ["BBB"] * 5,
            "day": [f"2026-01-0{index}" for index in range(1, 6)] * 2,
            "close": [10, 11, 12, 13, 14, 20, 22, 24, 26, 28],
        }
    )
    corr = correlation_matrix(daily)
    assert len(corr) == 1
    assert round(float(corr.iloc[0]["correlation"]), 6) == 1.0

    rates = pd.DataFrame(
        [
            {"day": "2026-01-02", "tenor": "2Y", "yield": 4.0},
            {"day": "2026-01-02", "tenor": "10Y", "yield": 4.5},
        ]
    )
    curve = yield_curve(rates)
    assert round(float(curve.iloc[0]["spread_10y_2y"]), 4) == 0.5


def test_trade_contract_validation_and_synthetic_stream() -> None:
    trade = make_trade("AAPL", "equity", _ms("2026-01-02T10:00:00Z"), 190.0, 2.0, "test")
    assert validate_trade(trade) == []

    bad = dict(trade)
    bad["price"] = -1
    assert "price must be > 0" in validate_trade(bad)

    missing = dict(trade)
    del missing["symbol"]
    assert "missing:symbol" in validate_trade(missing)

    stream = synthetic_trades(rows=50, seed=1)
    assert 0 < len(stream) <= 50
    assert all(validate_trade(record) == [] for record in stream)
