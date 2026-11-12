"""Finnhub trade websocket (backup feed for US stocks) -> ``stocks.trades``.

Protocol (finnhub.io/docs/api/websocket-trades): connect to
``wss://ws.finnhub.io?token=<key>``, send ``{"type":"subscribe","symbol":"AAPL"}`` per
symbol, then receive ``{"type":"trade","data":[{"s","p","v","t","c"}]}`` and periodic
``{"type":"ping"}``.

The previous version polled ``/quote`` and published volume 0, which made VWAP undefined
for every Finnhub bar; trades carry real sizes.
"""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Callable
from typing import Any

from common.contracts import INSTRUMENTS, make_trade

from ..base import BaseProducer, KafkaSink, now_ms
from ..config import ApiSettings
from .alpaca import DEFAULT_SYMBOLS

WS_URL = "wss://ws.finnhub.io"


def parse_finnhub_message(
    payload: dict[str, Any],
    ingest_time: int | None = None,
    on_reject: Callable[[Any, str], None] | None = None,
) -> list[dict[str, Any]]:
    """Finnhub websocket message -> contract trades (empty for ping/other types).

    Finnhub has no trade id, so ``trade_id`` falls back to the content hash: two trades
    of the same symbol with equal price, size and millisecond collapse into one. That
    loss is accepted for a backup feed.
    """
    if payload.get("type") != "trade":
        return []
    ingest_time = ingest_time if ingest_time is not None else now_ms()
    records: list[dict[str, Any]] = []
    for item in payload.get("data") or []:
        try:
            symbol = str(item["s"]).upper()
            instrument = INSTRUMENTS.get(symbol)
            records.append(
                make_trade(
                    symbol=symbol,
                    asset_class=instrument.asset_class if instrument else "equity",
                    event_time=int(item["t"]),
                    price=float(item["p"]),
                    volume=float(item.get("v") or 0.0),
                    source="finnhub",
                    ingest_time=ingest_time,
                    metadata={"conditions": item.get("c")},
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            if on_reject is not None:
                on_reject(item, f"finnhub trade: {exc!r}")
    return records


class FinnhubProducer(BaseProducer):
    source = "finnhub"

    def __init__(
        self,
        kafka: KafkaSink,
        settings: ApiSettings,
        symbols: list[str] | None = None,
        ws_url: str = WS_URL,
    ) -> None:
        super().__init__(kafka)
        if not settings.finnhub_key:
            raise RuntimeError("FINNHUB_API_KEY is required")
        self.settings = settings
        self.symbols = [symbol.upper() for symbol in (symbols or DEFAULT_SYMBOLS)]
        self.ws_url = ws_url

    def handle_message(self, raw: str | bytes) -> int:
        try:
            payload = json.loads(raw)
        except ValueError as exc:
            self.dead_letter(str(raw), f"bad json: {exc}", "stocks.trades")
            return 0
        records = parse_finnhub_message(
            payload,
            on_reject=lambda item, error: self.dead_letter(item, error, "stocks.trades"),
        )
        return sum(1 for record in records if self.publish(record))

    async def consume(self, stop_event: threading.Event) -> None:
        import websockets

        url = f"{self.ws_url}?token={self.settings.finnhub_key}"
        async with websockets.connect(url, ping_interval=20) as websocket:
            for symbol in self.symbols:
                await websocket.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            while not stop_event.is_set():
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                self.handle_message(raw)

    def run(self, stop_event: threading.Event) -> None:
        self.run_with_reconnect(self.consume, stop_event)
