"""Indicator/alert state across micro-batches (pure pandas, no Spark)."""

from __future__ import annotations

import datetime as dt

import pandas as pd

from common.alerts import detect_alerts
from common.indicators import add_indicators
from streaming.columns import ALERT_COLUMNS, INDICATOR_COLUMNS
from streaming.state import IndicatorState, alert_rows, indicator_rows

START = pd.Timestamp("2026-11-09T14:30:00Z")
NOW = pd.Timestamp("2026-11-09T15:10:00Z")


def _bar(minute: int, close: float, volume: float = 10.0, symbol: str = "SPY") -> dict:
    return {
        "symbol": symbol,
        "asset_class": "equity",
        "window_start": START + pd.Timedelta(minutes=minute),
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": volume,
        "vwap": close,
        "trade_count": 1,
    }


def _calm_then_spike(count: int = 30) -> list[dict]:
    # 30 calm minutes alternating 100/101 (std ~0.5), then a jump to 110 (~19 std).
    return [_bar(minute, 100.0 + minute % 2) for minute in range(count)] + [_bar(count, 110.0)]


# ROOT CAUSE:
#
# In update/append mode each micro-batch carries about one bar per symbol. The v1 job
# ran add_indicators + detect_alerts on that batch alone, so the 30-bar z-score had no
# history (min_periods=5) and was always NaN: no PRICE_SPIKE alert could ever fire,
# and SMA/RSI/MACD in Cassandra were meaningless.
#
#   enriched_pdf, alerts = enrich_batch(batch_df.toPandas())   # one bar of history
#
# We fixed this by keeping a per-symbol buffer of the last 60 bars on the driver
# (IndicatorState), restored from Cassandra after a restart, and computing indicators
# and alerts on that buffer.
def test_v1_per_batch_logic_never_alerts() -> None:
    alerts = []
    for bar in _calm_then_spike():
        enriched = add_indicators(pd.DataFrame([bar]))
        alerts.extend(detect_alerts(enriched))
    assert alerts == []


def test_state_alerts_on_spike_spread_over_31_batches() -> None:
    state = IndicatorState()
    updates = [state.update(pd.DataFrame([bar]), NOW) for bar in _calm_then_spike()]
    assert all(update.alerts.empty for update in updates[:-1])
    spike = updates[-1].alerts
    assert list(spike["alert_type"]) == ["PRICE_SPIKE"]
    assert spike.iloc[0]["severity"] == "high"
    assert spike.iloc[0]["zscore"] > 10
    assert spike.iloc[0]["alert_time"] == START + pd.Timedelta(minutes=30)
    latest = updates[-1].indicators.iloc[0]
    assert latest["close"] == 110.0
    assert latest["zscore_30m"] > 10
    assert pd.notna(latest["rsi_14"])
    assert pd.notna(latest["macd"])


def test_restart_restores_history_from_loader() -> None:
    history = pd.DataFrame(_calm_then_spike()[:-1])
    calls: list[tuple[str, pd.Timestamp]] = []

    def loader(symbol: str, as_of: pd.Timestamp) -> pd.DataFrame:
        calls.append((symbol, as_of))
        return history

    state = IndicatorState(loader=loader)
    update = state.update(pd.DataFrame([_bar(30, 110.0)]), NOW)
    assert list(update.alerts["alert_type"]) == ["PRICE_SPIKE"]
    state.update(pd.DataFrame([_bar(31, 101.0)]), NOW)
    # Hydration happens once per symbol per driver lifetime.
    assert calls == [("SPY", START + pd.Timedelta(minutes=30))]


def test_partial_bar_replaces_previous_version_and_buffer_is_bounded() -> None:
    state = IndicatorState(lookback=5)
    for minute in range(8):
        state.update(pd.DataFrame([_bar(minute, 100.0 + minute)]), NOW)
    state.update(pd.DataFrame([_bar(7, 120.0, volume=25.0)]), NOW)
    buffer = state.buffer("SPY")
    assert len(buffer) == 5
    assert list(buffer["window_start"]) == [START + pd.Timedelta(minutes=m) for m in range(3, 8)]
    assert buffer.iloc[-1]["close"] == 120.0
    assert buffer.iloc[-1]["volume"] == 25.0


def test_kafka_publication_is_deduplicated_by_alert_id() -> None:
    state = IndicatorState()
    for bar in _calm_then_spike()[:-1]:
        state.update(pd.DataFrame([bar]), NOW)
    first = state.update(pd.DataFrame([_bar(30, 110.0)]), NOW)
    # The open bar spikes again on the next trigger: same alert_id.
    again = state.update(pd.DataFrame([_bar(30, 111.0)]), NOW)
    assert len(first.new_alerts) == 1
    assert len(again.alerts) == 1  # still upserted to Cassandra with fresh values
    assert again.alerts.iloc[0]["price"] == 111.0
    assert again.new_alerts.empty
    assert first.alerts.iloc[0]["alert_id"] == again.alerts.iloc[0]["alert_id"]


def test_emitted_ids_are_bounded() -> None:
    state = IndicatorState(max_emitted=2)
    assert state._mark_emitted("a")
    assert state._mark_emitted("b")
    assert state._mark_emitted("c")  # evicts "a"
    assert not state._mark_emitted("c")
    assert state._mark_emitted("a")


def test_symbols_are_independent() -> None:
    state = IndicatorState()
    batch = pd.DataFrame([_bar(0, 100.0), _bar(0, 50.0, symbol="TLT")])
    update = state.update(batch, NOW)
    assert sorted(update.indicators["symbol"]) == ["SPY", "TLT"]
    assert len(state.buffer("TLT")) == 1


def test_frames_follow_contract_columns() -> None:
    update = IndicatorState().update(pd.DataFrame(_calm_then_spike()), NOW)
    assert list(update.indicators.columns) == INDICATOR_COLUMNS
    assert list(update.alerts.columns) == ALERT_COLUMNS
    assert update.indicators.iloc[0]["updated_at"] == NOW
    empty = IndicatorState().update(pd.DataFrame(), NOW)
    assert list(empty.indicators.columns) == INDICATOR_COLUMNS
    assert empty.alerts.empty


def test_indicator_rows_keep_newest_bar_per_symbol() -> None:
    bars = pd.DataFrame([_bar(0, 100.0), _bar(1, 102.0), _bar(0, 50.0, symbol="TLT")])
    rows = indicator_rows(bars, NOW)
    assert sorted(rows["symbol"]) == ["SPY", "TLT"]
    assert rows.set_index("symbol").loc["SPY", "close"] == 102.0


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
