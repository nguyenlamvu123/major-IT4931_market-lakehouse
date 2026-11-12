"""Binance public trade stream (crypto, 24/7) -> ``crypto.trades``.

One combined-stream connection carries every symbol
(``/stream?streams=btcusdt@trade/ethusdt@trade``), so adding a pair does not add a
socket. No API key is needed.
"""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any

from common.contracts import INSTRUMENTS, make_trade

from ..base import BaseProducer, KafkaSink, MalformedPayload, now_ms

DEFAULT_SYMBOLS = ("BTCUSDT", "ETHUSDT")
DEFAULT_WS_BASE = "wss://stream.binance.com:9443"


def parse_binance_trade(
    payload: dict[str, Any],
    ingest_time: int | None = None,
) -> dict[str, Any] | None:
    """Binance ``trade`` event -> contract trade, or None for other event types.

    Raises :class:`MalformedPayload` for a ``trade`` event missing fields, so the
    producer can send it to ``dead-letter`` instead of dropping it silently.

    Accepts both the raw stream payload and the combined-stream envelope
    ``{"stream": "...", "data": {...}}``. The exchange trade id ``t`` becomes
    ``exchange_trade_id`` so a trade re-sent after a reconnect keeps the same
    ``trade_id`` while two genuine trades with equal price/size stay distinct.
    """
    data = payload.get("data", payload)
    if data.get("e") != "trade":
        return None
    try:
        symbol = str(data["s"]).upper()
        instrument = INSTRUMENTS.get(symbol)
        return make_trade(
            symbol=symbol,
            # Pairs outside the registry (passed via --symbols) are still crypto.
            asset_class=instrument.asset_class if instrument else "crypto",
            event_time=int(data["T"]),
            price=float(data["p"]),
            volume=float(data["q"]),
            source="binance",
            exchange_trade_id=data["t"],
            ingest_time=ingest_time if ingest_time is not None else now_ms(),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise MalformedPayload(f"binance trade: {exc!r}") from exc


def combined_stream_url(symbols: list[str], ws_base: str = DEFAULT_WS_BASE) -> str:
    streams = "/".join(f"{symbol.lower()}@trade" for symbol in symbols)
    return f"{ws_base.rstrip('/')}/stream?streams={streams}"


class BinanceProducer(BaseProducer):
    source = "binance"

    def __init__(
        self,
        kafka: KafkaSink,
        symbols: list[str] | None = None,
        ws_base: str = DEFAULT_WS_BASE,
    ) -> None:
        super().__init__(kafka)
        self.symbols = [symbol.upper() for symbol in (symbols or DEFAULT_SYMBOLS)]
        self.url = combined_stream_url(self.symbols, ws_base)

    def handle_message(self, raw: str | bytes) -> int:
        """Publish the trade inside one websocket frame; return how many were sent."""
        try:
            payload = json.loads(raw)
            record = parse_binance_trade(payload)
        except (ValueError, AttributeError) as exc:  # bad JSON or MalformedPayload
            self.dead_letter(str(raw), str(exc), "crypto.trades")
            return 0
        if record is None:
            return 0
        return 1 if self.publish(record) else 0

    async def consume(self, stop_event: threading.Event) -> None:
        import websockets

        # Binance pings every 3 minutes and drops silent clients; the library answers
        # pings automatically, ping_interval adds our own keep-alive on top.
        async with websockets.connect(self.url, ping_interval=20) as websocket:
            while not stop_event.is_set():
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                self.handle_message(raw)

    def run(self, stop_event: threading.Event) -> None:
        self.run_with_reconnect(self.consume, stop_event)
