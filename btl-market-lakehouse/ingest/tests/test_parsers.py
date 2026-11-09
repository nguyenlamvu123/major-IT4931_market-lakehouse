"""Parser tests that do not require Kafka or market API keys."""

from __future__ import annotations

from ingest.producers.alpaca import parse_alpaca_trade
from ingest.producers.binance_ws import parse_binance_trade
from ingest.producers.finnhub import parse_finnhub_quote
from ingest.producers.treasury_fred import parse_fred_observations


def test_parse_binance_trade() -> None:
    record = parse_binance_trade(
        {"e": "trade", "s": "BTCUSDT", "T": 1767364200000, "p": "67000.5", "q": "0.25", "t": 1}
    )
    assert record is not None
    assert record["symbol"] == "BTCUSDT"
    assert record["asset_class"] == "crypto"
    assert record["event_time"] == 1767364200000


def test_parse_binance_ignores_non_trade() -> None:
    assert parse_binance_trade({"e": "depthUpdate"}) is None


def test_parse_alpaca_trade() -> None:
    record = parse_alpaca_trade(
        {"t": "2026-01-02T15:30:00Z", "p": 190.25, "s": 10, "x": "NASDAQ"},
        "AAPL",
    )
    assert record is not None
    assert record["symbol"] == "AAPL"
    assert record["volume"] == 10.0


def test_parse_finnhub_quote() -> None:
    record = parse_finnhub_quote({"c": 191.5, "t": 1767364200}, "AAPL")
    assert record is not None
    assert record["event_time"] == 1767364200000
    assert record["source"] == "finnhub"


def test_parse_fred_observations() -> None:
    records = parse_fred_observations(
        {
            "observations": [
                {"date": "2026-01-02", "value": "4.25"},
                {"date": "2026-01-03", "value": "."},
            ]
        },
        "10Y",
    )
    assert records == [{"day": "2026-01-02", "tenor": "10Y", "yield": 4.25, "source": "fred"}]
