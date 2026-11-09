"""Binance public websocket trade stream."""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any

from common.contracts import make_trade

from ..base import BaseProducer


def parse_binance_trade(payload: dict[str, Any]) -> dict[str, Any] | None:
    if payload.get("e") != "trade":
        return None
    try:
        return make_trade(
            symbol=str(payload["s"]).upper(),
            asset_class="crypto",
            event_time=int(payload["T"]),
            price=float(payload["p"]),
            volume=float(payload["q"]),
            source="binance",
            metadata={"trade_id_exchange": payload.get("t")},
        )
    except (KeyError, TypeError, ValueError):
        return None


class BinanceProducer(BaseProducer):
    def __init__(
        self,
        kafka: Any,
        symbols: list[str],
        ws_base: str = "wss://stream.binance.com:9443/ws",
    ) -> None:
        super().__init__(kafka)
        self.symbols = [symbol.upper() for symbol in symbols]
        self.ws_base = ws_base.rstrip("/")

    async def _consume(self, symbol: str, stop_event: threading.Event) -> None:
        import websockets

        url = f"{self.ws_base}/{symbol.lower()}@trade"
        async with websockets.connect(url, ping_interval=20) as websocket:
            while not stop_event.is_set():
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                record = parse_binance_trade(json.loads(raw))
                if record is not None:
                    self.kafka.send("crypto.trades", record["symbol"], record)

    async def _run_async(self, stop_event: threading.Event) -> None:
        await asyncio.gather(*(self._consume(symbol, stop_event) for symbol in self.symbols))

    def run(self, stop_event: threading.Event) -> None:
        asyncio.run(self._run_async(stop_event))
