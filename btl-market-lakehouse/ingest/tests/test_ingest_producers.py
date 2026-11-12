"""Producer parsers and routing against contract v2 (no network, no Kafka)."""

from __future__ import annotations

import json
import threading
from typing import Any

import pandas as pd
import pytest

from common.contracts import validate_bar, validate_rate, validate_trade
from ingest.base import iso_to_ms
from ingest.config import ApiSettings
from ingest.producers.alpaca import (
    AlpacaProducer,
    AlpacaRestPoller,
    AlpacaStreamError,
    parse_alpaca_ws_message,
)
from ingest.producers.binance_ws import BinanceProducer, combined_stream_url, parse_binance_trade
from ingest.producers.finnhub import parse_finnhub_message
from ingest.producers.mock import MockProducer
from ingest.producers.treasury_csv import (
    NOMINAL_COLUMNS,
    REAL_COLUMNS,
    TreasuryYieldProducer,
    months_to_fetch,
    parse_treasury_csv,
)
from ingest.producers.treasury_fred import parse_fred_observations
from ingest.producers.yahoo import latest_quotes

INGEST = 1_794_218_500_000


class FakeKafka:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict[str, Any]]] = []

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self.sent.append((topic, key, value))

    def flush(self, timeout: float = 10.0) -> None:
        del timeout


def test_iso_to_ms_handles_nanoseconds() -> None:
    # PDF journey example: 10:00:03.120 on 2026-11-09 -> event_time 1794218403120.
    assert iso_to_ms("2026-11-09T10:00:03.120Z") == 1_794_218_403_120
    assert iso_to_ms("2026-11-09T10:00:03.120456789Z") == 1_794_218_403_120
    assert iso_to_ms("2026-11-09T15:30:00Z") == 1_794_238_200_000


def test_binance_trade_matches_pdf_message_and_uses_exchange_id() -> None:
    payload = {
        "stream": "btcusdt@trade",
        "data": {"e": "trade", "s": "BTCUSDT", "t": 5001, "p": "92400.00", "q": "0.05",
                 "T": 1_794_218_403_120},
    }
    record = parse_binance_trade(payload, ingest_time=INGEST)
    assert record is not None
    assert validate_trade(record) == []
    assert record["symbol"] == "BTCUSDT"
    assert record["asset_class"] == "crypto"
    assert record["price"] == 92400.0
    assert record["volume"] == 0.05
    assert record["exchange_trade_id"] == "5001"
    assert record["ingest_time"] == INGEST
    twin = parse_binance_trade(dict(payload["data"], t=5002), ingest_time=INGEST)
    assert twin is not None and twin["trade_id"] != record["trade_id"]
    assert parse_binance_trade({"e": "depthUpdate"}) is None


def test_binance_publishes_to_crypto_trades_keyed_by_symbol() -> None:
    kafka = FakeKafka()
    producer = BinanceProducer(kafka)
    assert producer.url.endswith("/stream?streams=btcusdt@trade/ethusdt@trade")
    assert combined_stream_url(["SOLUSDT"]).endswith("solusdt@trade")
    producer.handle_message(json.dumps(
        {"e": "trade", "s": "ETHUSDT", "t": 7, "p": "3200", "q": "1", "T": 1_794_218_403_000}
    ))
    assert [(topic, key) for topic, key, _ in kafka.sent] == [("crypto.trades", "ETHUSDT")]


def test_alpaca_ws_trades_and_bars_route_by_registry() -> None:
    frame = json.dumps(
        [
            {"T": "success", "msg": "authenticated"},
            {"T": "t", "S": "GLD", "i": 96921, "x": "V", "p": 250.55, "s": 100,
             "t": "2026-11-09T15:30:00.123456789Z", "z": "B"},
            {"T": "t", "S": "TLT", "i": 3, "x": "V", "p": 92.1, "s": 5,
             "t": "2026-11-09T15:30:01Z", "z": "C"},
            {"T": "b", "S": "SPY", "o": 590.0, "h": 590.5, "l": 589.8, "c": 590.2,
             "v": 49378, "t": "2026-11-09T15:30:00Z", "n": 461, "vw": 590.1},
        ]
    )
    kafka = FakeKafka()
    producer = AlpacaProducer(kafka, ApiSettings(alpaca_key="k", alpaca_secret="s"))
    assert producer.handle_message(frame) == 3
    sent = {(topic, key): value for topic, key, value in kafka.sent}
    gld = sent[("stocks.trades", "GLD")]
    assert gld["asset_class"] == "gold"
    assert gld["exchange_trade_id"] == "96921"
    assert gld["event_time"] == 1_794_238_200_123
    assert validate_trade(gld) == []
    assert sent[("stocks.trades", "TLT")]["asset_class"] == "bond"
    bar = sent[("stocks.bars", "SPY")]
    assert bar["timeframe"] == "1Min"
    assert bar["trade_count"] == 461
    assert validate_bar(bar) == []


def test_alpaca_ws_error_raises_for_backoff() -> None:
    with pytest.raises(AlpacaStreamError):
        parse_alpaca_ws_message(json.dumps([{"T": "error", "code": 406,
                                             "msg": "connection limit exceeded"}]))


class FakeAlpacaRest:
    """Alpaca ``/v2/stocks/{symbol}/trades`` semantics for one symbol.

    Without ``start`` the API begins at the start of the current day and returns trades
    in ascending order, ``limit`` per page with a ``next_page_token``.
    """

    def __init__(self, trades: list[dict[str, Any]]) -> None:
        self.trades = trades
        self.calls: list[dict[str, Any]] = []

    def __call__(self, url: str, params: dict[str, Any], **_: Any) -> Any:
        self.calls.append(dict(params))
        start = params.get("start")
        rows = [t for t in self.trades if start is None or iso_to_ms(t["t"]) >= iso_to_ms(start)]
        offset = int(params.get("page_token") or 0)
        limit = int(params.get("limit", 1000))
        page = rows[offset : offset + limit]
        token = str(offset + limit) if offset + limit < len(rows) else None
        body = {"trades": page, "next_page_token": token}

        class Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, Any]:
                return body

        return Response()


def _alpaca_trade(index: int) -> dict[str, Any]:
    return {"t": f"2026-11-09T14:30:{index:02d}Z", "p": 230.0 + index / 100, "s": 10,
            "i": index, "x": "V"}


# ROOT CAUSE:
#
# The old AlpacaProducer._poll sent only {"limit": 100}. Alpaca then starts at the
# beginning of the day, so every poll returned the same first 100 trades; they were all
# in the `_seen` set and nothing new was ever published.
#
# We fixed this by sending feed=iex, a per-symbol start cursor and following
# next_page_token; trades at the inclusive cursor are filtered by trade_id.
def test_alpaca_rest_poll_advances_past_first_page_of_the_day() -> None:
    server = FakeAlpacaRest([_alpaca_trade(index) for index in range(5)])
    poller = AlpacaRestPoller(
        ApiSettings(alpaca_key="k", alpaca_secret="s"),
        http_get=server,
        page_limit=2,
        clock_ms=lambda: iso_to_ms("2026-11-09T14:30:30Z"),
        lookback_seconds=60,
    )
    first = poller.fetch_new_trades("AAPL")
    assert [record["exchange_trade_id"] for record in first] == ["0", "1", "2", "3", "4"]
    assert all(call["feed"] == "iex" for call in server.calls)
    assert server.calls[0]["start"].startswith("2026-11-09T14:29:30")
    assert server.calls[1]["page_token"] == "2"

    server.trades.extend(_alpaca_trade(index) for index in range(5, 8))
    second = poller.fetch_new_trades("AAPL")
    assert [record["exchange_trade_id"] for record in second] == ["5", "6", "7"]
    assert server.calls[-1]["start"] == "2026-11-09T14:30:04Z"
    assert poller.fetch_new_trades("AAPL") == []


def test_finnhub_trades_have_real_volume() -> None:
    message = {"type": "trade", "data": [
        {"s": "AAPL", "p": 231.4, "t": 1_794_238_200_500, "v": 25, "c": ["1"]},
        {"s": "SHY", "p": 82.1, "t": 1_794_238_200_600, "v": 3},
    ]}
    records = parse_finnhub_message(message, ingest_time=INGEST)
    assert [record["volume"] for record in records] == [25.0, 3.0]
    assert records[1]["asset_class"] == "bond"
    assert all(validate_trade(record) == [] for record in records)
    assert parse_finnhub_message({"type": "ping"}) == []


def test_yahoo_maps_gold_and_dxy_to_their_topics() -> None:
    index = pd.DatetimeIndex(
        ["2026-11-09T15:29:00Z", "2026-11-09T15:30:00Z"], name="Datetime"
    )
    frame = pd.concat(
        {
            "GC=F": pd.DataFrame({"Close": [2650.0, 2651.5], "Volume": [120.0, 80.0]},
                                 index=index),
            "DX-Y.NYB": pd.DataFrame({"Close": [105.1, float("nan")], "Volume": [0.0, 0.0]},
                                     index=index),
        },
        axis=1,
    )
    records = latest_quotes(frame, ["GC=F", "DXY"], ingest_time=INGEST)
    by_symbol = {record["symbol"]: record for record in records}
    assert by_symbol["GC=F"]["asset_class"] == "gold"
    assert by_symbol["GC=F"]["price"] == 2651.5
    assert by_symbol["GC=F"]["event_time"] == 1_794_238_200_000
    # The NaN row of the still-forming minute is skipped, not published as price NaN.
    assert by_symbol["DXY"]["price"] == 105.1
    assert by_symbol["DXY"]["volume"] == 0.0
    assert by_symbol["DXY"]["asset_class"] == "fx"
    assert all(validate_trade(record) == [] for record in records)

    kafka = FakeKafka()
    producer = MockProducer(kafka)
    for record in records:
        producer.publish(record)
    assert sorted(topic for topic, _, _ in kafka.sent) == ["fx.dxy", "gold.quotes"]


def test_fred_publishes_all_contract_series() -> None:
    payload = {"observations": [{"date": "2026-11-06", "value": "1.85"},
                                {"date": "2026-11-11", "value": "."}]}
    records = parse_fred_observations(payload, "DFII10", ingest_time=INGEST)
    assert len(records) == 1
    assert records[0]["tenor"] == "REAL10Y"
    assert validate_rate(records[0]) == []


TREASURY_NOMINAL = (
    '﻿"Date","1 Mo","1.5 Month","2 Mo","3 Mo","4 Mo","6 Mo","1 Yr","2 Yr","3 Yr",'
    '"5 Yr","7 Yr","10 Yr","20 Yr","30 Yr"\n'
    "11/10/2026,4.40,4.38,4.36,4.33,4.30,4.25,4.12,4.02,3.98,4.00,4.08,4.15,4.45,4.47\n"
    "11/09/2026,4.41,,4.37,4.34,4.31,4.26,4.13,4.03,3.99,4.01,4.09,4.16,4.46,4.48\n"
)
TREASURY_REAL = '"Date","5 YR","7 YR","10 YR","20 YR","30 YR"\n11/10/2026,1.40,1.60,1.84,2.10,2.20\n'


def test_treasury_csv_maps_columns_to_contract_series() -> None:
    nominal = parse_treasury_csv(TREASURY_NOMINAL, NOMINAL_COLUMNS, ingest_time=INGEST)
    assert len(nominal) == 16
    ten = [r for r in nominal if r["series_id"] == "DGS10" and r["day"] == "2026-11-10"]
    assert ten[0]["yield"] == 4.15 and ten[0]["source"] == "treasury"
    real = parse_treasury_csv(TREASURY_REAL, REAL_COLUMNS, ingest_time=INGEST)
    assert [(r["series_id"], r["tenor"], r["yield"]) for r in real] == [("DFII10", "REAL10Y", 1.84)]
    assert all(validate_rate(record) == [] for record in nominal + real)


def test_treasury_publishes_each_day_once_keyed_by_tenor() -> None:
    class Response:
        def __init__(self, text: str) -> None:
            self.text = text

        def raise_for_status(self) -> None:
            return None

    def http_get(url: str, params: dict[str, Any], **_: Any) -> Response:
        return Response(TREASURY_REAL if "real" in params["type"] else TREASURY_NOMINAL)

    kafka = FakeKafka()
    producer = TreasuryYieldProducer(kafka, lookback_days=1, http_get=http_get)
    producer.poll_once()
    first = len(kafka.sent)
    assert first > 0
    assert {topic for topic, _, _ in kafka.sent} == {"rates.yields"}
    assert {key for _, key, _ in kafka.sent} >= {"10Y", "REAL10Y", "1M"}
    producer.poll_once()
    assert len(kafka.sent) == first


def test_months_to_fetch_crosses_year_boundary() -> None:
    from datetime import date

    assert months_to_fetch(date(2027, 1, 3), 10) == ["202612", "202701"]


def test_mock_covers_every_input_topic() -> None:
    kafka = FakeKafka()
    MockProducer(kafka, rows=400, interval=0.0, days=3).run(threading.Event())
    topics = {topic for topic, _, _ in kafka.sent}
    assert topics == {"crypto.trades", "stocks.trades", "stocks.bars", "gold.quotes",
                      "fx.dxy", "rates.yields"}
    for topic, key, value in kafka.sent:
        assert "ingest_time" in value
        expected_key = value["tenor"] if topic == "rates.yields" else value["symbol"]
        assert key == expected_key
        if topic == "stocks.bars":
            assert validate_bar(value) == []
        elif topic == "rates.yields":
            assert validate_rate(value) == []
        else:
            assert validate_trade(value) == []
