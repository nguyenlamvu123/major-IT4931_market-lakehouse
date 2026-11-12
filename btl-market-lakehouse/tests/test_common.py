"""Tests for the shared pure-Python pipeline logic and contract v2 helpers."""

from __future__ import annotations

import pandas as pd
import pytest

from common.alerts import detect_alerts, make_alert
from common.analytics import (
    aligned_daily_panel,
    correlation_matrix,
    daily_summary,
    yield_curve,
)
from common.contracts import (
    INSTRUMENTS,
    TOPICS,
    make_bar,
    make_rate,
    make_trade,
    synthetic_trades,
    synthetic_yields,
    topic_for_record,
    validate_bar,
    validate_rate,
    validate_trade,
)
from common.indicators import add_indicators, rolling_zscore
from common.ohlcv import aggregate_trades


def _ms(value: str) -> int:
    return int(pd.Timestamp(value).timestamp() * 1000)


def test_topics_match_pdf_contract() -> None:
    # "Hợp đồng giữa các vai" in BTL-IT4931_Kien-truc-Phan-cong-GitHub.pdf, page 5.
    pdf_topics = {
        "crypto.trades",
        "stocks.trades",
        "stocks.bars",
        "gold.quotes",
        "fx.dxy",
        "rates.yields",
        "alerts",
    }
    assert pdf_topics <= set(TOPICS)
    assert set(TOPICS) - pdf_topics == {"dead-letter"}


def test_routing_uses_feed_topic_and_underlying_asset_class() -> None:
    gld = make_trade("GLD", "gold", _ms("2026-11-09T15:00:00Z"), 250.0, 10, "alpaca")
    assert topic_for_record(gld) == "stocks.trades"
    assert INSTRUMENTS["TLT"].asset_class == "bond"
    assert topic_for_record({"symbol": "GC=F", "asset_class": "gold"}) == "gold.quotes"
    assert topic_for_record({"symbol": "DXY", "asset_class": "fx"}) == "fx.dxy"
    assert topic_for_record({"symbol": "BTCUSDT", "asset_class": "crypto"}) == "crypto.trades"
    assert topic_for_record(make_rate("DGS10", "2026-11-09", 4.2, "fred")) == "rates.yields"
    bar = make_bar("SPY", "equity", "1Min", _ms("2026-11-09T15:00:00Z"), 1, 2, 0.5, 1.5, 10, "x")
    assert topic_for_record(bar) == "stocks.bars"
    assert topic_for_record(make_alert("SPY", "2026-11-09T15:00:00+00:00", "PRICE_SPIKE",
                                       "high", 1.0, "m")) == "alerts"
    with pytest.raises(KeyError):
        topic_for_record({"symbol": "UNKNOWN", "asset_class": "rates"})


def test_trade_id_prefers_exchange_id() -> None:
    event_time = _ms("2026-11-09T10:00:03.120Z")
    first = make_trade("BTCUSDT", "crypto", event_time, 92400.0, 0.05, "binance",
                       exchange_trade_id=1)
    second = make_trade("BTCUSDT", "crypto", event_time, 92400.0, 0.05, "binance",
                        exchange_trade_id=2)
    resent = make_trade("BTCUSDT", "crypto", event_time, 92400.0, 0.05, "binance",
                        exchange_trade_id=1)
    # Two genuine trades with identical time/price/size must stay distinct...
    assert first["trade_id"] != second["trade_id"]
    # ...while a re-sent trade after a reconnect collapses to the same key.
    assert first["trade_id"] == resent["trade_id"]


def test_aggregate_trades_ohlcv_vwap_and_dedupe() -> None:
    first = make_trade("BTCUSDT", "crypto", _ms("2026-01-02T10:00:00Z"), 100.0, 1.0, "t",
                       exchange_trade_id=1)
    trades = pd.DataFrame(
        [
            first,
            first,  # re-sent after reconnect: must not double the volume
            make_trade("BTCUSDT", "crypto", _ms("2026-01-02T10:00:30Z"), 101.0, 2.0, "t",
                       exchange_trade_id=2),
            make_trade("BTCUSDT", "crypto", _ms("2026-01-02T10:01:10Z"), 99.0, 3.0, "t",
                       exchange_trade_id=3),
        ]
    )
    bars = aggregate_trades(trades)
    assert len(bars) == 2
    first_bar = bars.iloc[0]
    assert first_bar["open"] == 100.0
    assert first_bar["high"] == 101.0
    assert first_bar["low"] == 100.0
    assert first_bar["close"] == 101.0
    assert first_bar["volume"] == 3.0
    assert first_bar["trade_count"] == 2
    assert first_bar["asset_class"] == "crypto"
    assert round(float(first_bar["vwap"]), 4) == round((100 + 202) / 3, 4)


def test_vwap_is_empty_without_volume() -> None:
    trades = pd.DataFrame(
        [make_trade("DXY", "fx", _ms("2026-01-02T10:00:00Z"), 105.0, 0.0, "yahoo")]
    )
    bars = aggregate_trades(trades)
    assert pd.isna(bars.iloc[0]["vwap"])


def test_zscore_baseline_excludes_current_bar() -> None:
    series = pd.Series([100.0, 101.0] * 15 + [103.0])
    zscore = rolling_zscore(series, window=30)
    # Baseline mean 100.5, std ~0.509 -> about 4.9 std away. Including the spike in its
    # own baseline would shrink it to ~3.7.
    assert zscore.iloc[-1] > 4.5


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
                "asset_class": "equity",
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
    assert all(alert["day"] == "2026-01-02" for alert in alerts)
    assert all(alert["asset_class"] == "equity" for alert in alerts)


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
    assert corr.iloc[0]["n_obs"] == 4
    assert corr.iloc[0]["as_of"] == "2026-01-05"

    rates = pd.DataFrame(
        [
            {"day": "2026-01-02", "tenor": "2Y", "yield": 4.5},
            {"day": "2026-01-02", "tenor": "10Y", "yield": 4.0},
            {"day": "2026-01-02", "tenor": "3M", "yield": 4.6},
        ]
    )
    curve = yield_curve(rates)
    assert round(float(curve.iloc[0]["spread_10y_2y"]), 4) == -0.5
    assert round(float(curve.iloc[0]["spread_10y_3m"]), 4) == -0.6
    assert bool(curve.iloc[0]["inverted"]) is True
    assert pd.isna(curve.iloc[0]["y_30y"])


def test_calendar_alignment_drops_crypto_weekends_and_ffills_bond_holidays() -> None:
    # Mon 2026-10-12 is Columbus Day: stocks trade, the bond market is closed.
    stock_days = ["2026-10-08", "2026-10-09", "2026-10-12", "2026-10-13"]
    crypto_days = ["2026-10-08", "2026-10-09", "2026-10-10", "2026-10-11", "2026-10-12",
                   "2026-10-13"]
    daily = pd.DataFrame(
        [{"symbol": "SPY", "day": day, "close": 100.0 + index}
         for index, day in enumerate(stock_days)]
        + [{"symbol": "BTCUSDT", "day": day, "close": 90_000.0 + 10 * index}
           for index, day in enumerate(crypto_days)]
    )
    rates = pd.DataFrame(
        [make_rate("DGS10", day, 4.0 + index / 100, "fred")
         for index, day in enumerate(["2026-10-08", "2026-10-09", "2026-10-13"])]
    )
    panel = aligned_daily_panel(daily, rates)
    assert list(panel.index) == stock_days
    # Columbus Day carries Friday's 10Y level forward instead of leaving a hole.
    assert panel.loc["2026-10-12", "US10Y"] == panel.loc["2026-10-09", "US10Y"]
    assert panel.loc["2026-10-12", "BTCUSDT"] == 90_040.0


def test_synthetic_market_answers_pdf_questions() -> None:
    days = 90
    trades = pd.DataFrame(synthetic_trades(rows=6_000, seed=42, days=days))
    daily = daily_summary(aggregate_trades(trades))
    rates = pd.DataFrame(synthetic_yields(days=days, seed=42))
    corr = correlation_matrix(daily, rates=rates)

    def pair(base: str, quote: str) -> float:
        low, high = sorted((base, quote))
        row = corr[(corr["base_symbol"] == low) & (corr["quote_symbol"] == high)]
        return float(row.iloc[0]["correlation"])

    assert pair("TLT", "US10Y") < -0.5
    assert pair("GLD", "REAL10Y") < -0.3
    assert pair("SPY", "AAPL") > 0.3


def test_trade_contract_validation_and_synthetic_stream() -> None:
    trade = make_trade("AAPL", "equity", _ms("2026-01-02T10:00:00Z"), 190.0, 2.0, "test")
    assert validate_trade(trade) == []

    bad = dict(trade)
    bad["price"] = -1
    assert "price must be > 0" in validate_trade(bad)

    missing = dict(trade)
    del missing["symbol"]
    assert "missing:symbol" in validate_trade(missing)

    as_rates = dict(trade, asset_class="rates")
    assert "unknown asset_class: rates" in validate_trade(as_rates)

    stream = synthetic_trades(rows=50, seed=1)
    assert 0 < len(stream) <= 50
    assert all(validate_trade(record) == [] for record in stream)


def test_bar_and_rate_validation() -> None:
    bar = make_bar("SPY", "equity", "1Min", _ms("2026-11-09T15:00:00Z"),
                   590.0, 591.0, 589.5, 590.5, 1200, "alpaca")
    assert validate_bar(bar) == []
    assert "high/low do not bound open/close" in validate_bar(dict(bar, high=589.0))

    rate = make_rate("DFII10", "2026-11-09", 1.85, "fred")
    assert rate["tenor"] == "REAL10Y"
    assert validate_rate(rate) == []
    assert "series_id does not match tenor" in validate_rate(dict(rate, tenor="10Y"))
    assert all(validate_rate(record) == [] for record in synthetic_yields(days=10))
