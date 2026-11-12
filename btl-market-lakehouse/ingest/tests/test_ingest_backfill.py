"""yfinance daily history -> stocks.bars 1Day records (fake frames, no network)."""

from __future__ import annotations

import threading
from datetime import date
from typing import Any

import pandas as pd

from common.contracts import day_to_epoch_ms, validate_bar
from ingest.backfill_yfinance import BackfillProducer, backfill_ticker, history_to_bars
from ingest.run_producer import build_source, parse_args


class FakeKafka:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict[str, Any]]] = []

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self.sent.append((topic, key, value))

    def flush(self, timeout: float = 10.0) -> None:
        del timeout


def _history(tz: str) -> pd.DataFrame:
    index = pd.DatetimeIndex(
        [pd.Timestamp("2026-11-06", tz=tz), pd.Timestamp("2026-11-09", tz=tz),
         pd.Timestamp("2026-11-10", tz=tz)],
        name="Date",
    )
    return pd.DataFrame(
        {
            "Open": [92.0, 92.4, float("nan")],
            "High": [92.6, 92.9, 93.0],
            "Low": [91.8, 92.1, 92.2],
            "Close": [92.3, 92.7, 92.5],
            "Volume": [1.2e7, float("nan"), 1.0e7],
        },
        index=index,
    )


def test_history_to_bars_uses_exchange_trading_day() -> None:
    bars = history_to_bars(_history("America/New_York"), "TLT", ingest_time=1)
    assert [bar["event_time"] for bar in bars] == [
        day_to_epoch_ms("2026-11-06"),
        day_to_epoch_ms("2026-11-09"),
    ]
    assert all(bar["timeframe"] == "1Day" for bar in bars)
    assert all(bar["asset_class"] == "bond" for bar in bars)
    assert bars[1]["volume"] == 0.0  # missing volume is not a reason to drop the day
    assert all(validate_bar(bar) == [] for bar in bars)


def test_backfill_tickers_cover_crypto_and_dxy() -> None:
    assert backfill_ticker("BTCUSDT") == "BTC-USD"
    assert backfill_ticker("DXY") == "DX-Y.NYB"
    assert backfill_ticker("GC=F") == "GC=F"
    assert backfill_ticker("SPY") == "SPY"


def test_backfill_publishes_one_bar_per_day_to_stocks_bars() -> None:
    class Producer(BackfillProducer):
        def history(self, symbol: str) -> pd.DataFrame:
            return _history("UTC" if symbol == "BTCUSDT" else "America/New_York")

    kafka = FakeKafka()
    Producer(kafka, ["BTCUSDT", "GC=F"], start=date(2026, 11, 1), end=date(2026, 11, 11)).run(
        threading.Event()
    )
    assert {topic for topic, _, _ in kafka.sent} == {"stocks.bars"}
    assert [key for _, key, _ in kafka.sent] == ["BTCUSDT", "BTCUSDT", "GC=F", "GC=F"]
    assert kafka.sent[0][2]["asset_class"] == "crypto"
    assert kafka.sent[2][2]["asset_class"] == "gold"


def test_run_producer_builds_backfill_with_dates() -> None:
    args = parse_args(["--source", "backfill", "--start", "2021-01-04", "--symbols", "spy"])
    producer = build_source(args, FakeKafka())
    assert isinstance(producer, BackfillProducer)
    assert producer.start == date(2021, 1, 4)
    assert producer.symbols == ["SPY"]
