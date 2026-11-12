"""Alpaca market data (free IEX feed) -> ``stocks.trades`` and ``stocks.bars``.

Primary mode is the IEX websocket. Protocol (docs.alpaca.markets, "Real-time Stock
Data"):

1. server sends ``[{"T":"success","msg":"connected"}]``
2. client sends ``{"action":"auth","key":...,"secret":...}``
   -> ``[{"T":"success","msg":"authenticated"}]`` or ``[{"T":"error",...}]``
3. client sends ``{"action":"subscribe","trades":[...],"bars":[...]}``
4. server streams arrays of ``{"T":"t",...}`` trades and ``{"T":"b",...}`` 1-minute bars.

The REST mode is a fallback for when the websocket is unavailable (connection limit of
the free plan, firewall). It must page forward from the last trade seen; see
:class:`AlpacaRestPoller`.

The free plan only exposes IEX, which carries a small share of US volume (noted as out of
scope in the idea PDF). Prices are representative, volumes are not consolidated.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from common.contracts import INSTRUMENTS, make_bar, make_trade

from ..base import BaseProducer, KafkaSink, iso_to_ms, now_ms
from ..config import ApiSettings

LOGGER = logging.getLogger(__name__)

#: stocks.trades symbols from the contract registry (equities, GLD, bond ETFs).
DEFAULT_SYMBOLS = tuple(
    symbol for symbol, instrument in INSTRUMENTS.items() if instrument.topic == "stocks.trades"
)
WS_URL = "wss://stream.data.alpaca.markets/v2/iex"
REST_BASE = "https://data.alpaca.markets/v2/stocks"


def _asset_class(symbol: str) -> str:
    instrument = INSTRUMENTS.get(symbol)
    return instrument.asset_class if instrument else "equity"


def parse_alpaca_trade(
    payload: dict[str, Any],
    symbol: str | None = None,
    ingest_time: int | None = None,
) -> dict[str, Any] | None:
    """Alpaca trade (websocket ``T=t`` or REST item) -> contract trade.

    The websocket carries the symbol in ``S``; REST items do not, so the caller passes
    it. ``i`` is Alpaca's per-symbol trade id and becomes ``exchange_trade_id``.
    """
    try:
        resolved = str(payload.get("S") or symbol).upper()
        return make_trade(
            symbol=resolved,
            asset_class=_asset_class(resolved),
            event_time=iso_to_ms(str(payload["t"])),
            price=float(payload["p"]),
            volume=float(payload.get("s", 0)),
            source="alpaca",
            exchange_trade_id=payload.get("i"),
            ingest_time=ingest_time if ingest_time is not None else now_ms(),
            metadata={"exchange": payload.get("x"), "tape": payload.get("z")},
        )
    except (KeyError, TypeError, ValueError):
        return None


def parse_alpaca_bar(
    payload: dict[str, Any],
    ingest_time: int | None = None,
) -> dict[str, Any] | None:
    """Alpaca 1-minute bar (websocket ``T=b``) -> contract bar on ``stocks.bars``."""
    try:
        symbol = str(payload["S"]).upper()
        return make_bar(
            symbol=symbol,
            asset_class=_asset_class(symbol),
            timeframe="1Min",
            event_time=iso_to_ms(str(payload["t"])),
            open_=float(payload["o"]),
            high=float(payload["h"]),
            low=float(payload["l"]),
            close=float(payload["c"]),
            volume=float(payload["v"]),
            source="alpaca",
            vwap=float(payload["vw"]) if payload.get("vw") is not None else None,
            trade_count=int(payload["n"]) if payload.get("n") is not None else None,
            ingest_time=ingest_time if ingest_time is not None else now_ms(),
        )
    except (KeyError, TypeError, ValueError):
        return None


class AlpacaStreamError(RuntimeError):
    """Alpaca sent ``T=error`` (bad credentials, connection limit, invalid syntax)."""


def parse_alpaca_ws_message(raw: str | bytes, ingest_time: int | None = None) -> list[dict]:
    """One websocket frame (a JSON array) -> contract records.

    Control messages (``success``, ``subscription``) are skipped; ``error`` raises so the
    reconnect loop backs off instead of spinning on a rejected session.
    """
    ingest_time = ingest_time if ingest_time is not None else now_ms()
    messages = json.loads(raw)
    if isinstance(messages, dict):
        messages = [messages]
    records: list[dict[str, Any]] = []
    for message in messages:
        kind = message.get("T")
        if kind == "t":
            record = parse_alpaca_trade(message, ingest_time=ingest_time)
        elif kind == "b":
            record = parse_alpaca_bar(message, ingest_time=ingest_time)
        elif kind == "error":
            raise AlpacaStreamError(f"{message.get('code')}: {message.get('msg')}")
        else:
            continue
        if record is not None:
            records.append(record)
    return records


HttpGet = Callable[..., Any]


class AlpacaRestPoller:
    """Poll ``/v2/stocks/{symbol}/trades`` forward in time.

    Bug fixed here (review of 12/11/2026): the old poller called the endpoint with only
    ``limit=100``. Alpaca then defaults ``start`` to the beginning of the current day and
    returns trades in ascending order, so every poll returned the same first 100 trades
    of the day and no new trade ever reached Kafka. Without ``feed=iex`` a free account
    is also refused recent SIP data.

    Now each symbol keeps a cursor at the newest trade timestamp seen; the next poll
    starts there (inclusive, so the boundary trade is filtered by ``trade_id``) and
    follows ``next_page_token`` until the backlog is drained.
    """

    def __init__(
        self,
        settings: ApiSettings,
        http_get: HttpGet | None = None,
        lookback_seconds: int = 60,
        page_limit: int = 1000,
        clock_ms: Callable[[], int] = now_ms,
    ) -> None:
        if http_get is None:
            import requests

            http_get = requests.get
        self.settings = settings
        self.http_get = http_get
        self.page_limit = page_limit
        self.clock_ms = clock_ms
        self.lookback_ms = lookback_seconds * 1000
        #: symbol -> RFC 3339 timestamp of the newest trade already published.
        self.cursor: dict[str, str] = {}
        #: trade_ids published at exactly the cursor timestamp (inclusive start).
        self._boundary_ids: dict[str, set[str]] = {}

    def _start_for(self, symbol: str) -> str:
        if symbol in self.cursor:
            return self.cursor[symbol]
        start_ms = self.clock_ms() - self.lookback_ms
        start = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        return start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def fetch_new_trades(self, symbol: str) -> list[dict[str, Any]]:
        symbol = symbol.upper()
        start = self._start_for(symbol)
        params: dict[str, Any] = {
            "feed": "iex",
            "start": start,
            "limit": self.page_limit,
            "sort": "asc",
        }
        headers = {
            "APCA-API-KEY-ID": self.settings.alpaca_key,
            "APCA-API-SECRET-KEY": self.settings.alpaca_secret,
        }
        boundary = self._boundary_ids.get(symbol, set())
        output: list[dict[str, Any]] = []
        newest_raw: str | None = None
        while True:
            response = self.http_get(
                f"{REST_BASE}/{symbol}/trades", params=dict(params), headers=headers, timeout=10
            )
            response.raise_for_status()
            body = response.json()
            for item in body.get("trades") or []:
                record = parse_alpaca_trade(item, symbol)
                if record is None or record["trade_id"] in boundary:
                    continue
                output.append(record)
                newest_raw = str(item["t"])
            token = body.get("next_page_token")
            if not token:
                break
            params["page_token"] = token

        if newest_raw is not None:
            newest_ms = iso_to_ms(newest_raw)
            self.cursor[symbol] = newest_raw
            self._boundary_ids[symbol] = {
                record["trade_id"] for record in output if record["event_time"] == newest_ms
            }
        return output


class AlpacaProducer(BaseProducer):
    source = "alpaca"

    def __init__(
        self,
        kafka: KafkaSink,
        settings: ApiSettings,
        symbols: list[str] | None = None,
        mode: str = "ws",
        interval: float = 15.0,
        ws_url: str = WS_URL,
    ) -> None:
        super().__init__(kafka)
        if not settings.alpaca_key or not settings.alpaca_secret:
            raise RuntimeError("ALPACA_API_KEY and ALPACA_SECRET_KEY are required")
        if mode not in {"ws", "rest"}:
            raise ValueError(f"unknown Alpaca mode {mode!r}")
        self.settings = settings
        self.symbols = [symbol.upper() for symbol in (symbols or DEFAULT_SYMBOLS)]
        self.mode = mode
        self.interval = interval
        self.ws_url = ws_url
        self.poller = AlpacaRestPoller(settings) if mode == "rest" else None

    def handle_message(self, raw: str | bytes) -> int:
        records = parse_alpaca_ws_message(raw)
        for record in records:
            self.publish(record)
        return len(records)

    async def consume(self, stop_event: threading.Event) -> None:
        import websockets

        async with websockets.connect(self.ws_url, ping_interval=20) as websocket:
            await websocket.send(
                json.dumps(
                    {
                        "action": "auth",
                        "key": self.settings.alpaca_key,
                        "secret": self.settings.alpaca_secret,
                    }
                )
            )
            await websocket.send(
                json.dumps({"action": "subscribe", "trades": self.symbols, "bars": self.symbols})
            )
            while not stop_event.is_set():
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                self.handle_message(raw)

    def poll_once(self) -> int:
        assert self.poller is not None
        sent = 0
        for symbol in self.symbols:
            for record in self.poller.fetch_new_trades(symbol):
                self.publish(record)
                sent += 1
        return sent

    def run(self, stop_event: threading.Event) -> None:
        if self.mode == "ws":
            asyncio.run(self.consume(stop_event))
            return
        while not stop_event.is_set():
            self.poll_once()
            stop_event.wait(self.interval)
