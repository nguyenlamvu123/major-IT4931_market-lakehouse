"""Alpaca market-data REST poller."""

from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any

import requests

from common.contracts import make_trade

from ..base import BaseProducer
from ..config import ApiSettings


def _iso_to_ms(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def parse_alpaca_trade(payload: dict[str, Any], symbol: str) -> dict[str, Any] | None:
    try:
        return make_trade(
            symbol=symbol.upper(),
            asset_class="equity",
            event_time=_iso_to_ms(str(payload["t"])),
            price=float(payload["p"]),
            volume=float(payload.get("s", 0)),
            source="alpaca",
            metadata={"exchange": payload.get("x")},
        )
    except (KeyError, TypeError, ValueError):
        return None


class AlpacaProducer(BaseProducer):
    def __init__(
        self,
        kafka: Any,
        settings: ApiSettings,
        symbols: list[str],
        interval: float = 15.0,
    ) -> None:
        super().__init__(kafka)
        if not settings.alpaca_key or not settings.alpaca_secret:
            raise RuntimeError("ALPACA_API_KEY and ALPACA_SECRET_KEY are required")
        self.settings = settings
        self.symbols = [symbol.upper() for symbol in symbols]
        self.interval = interval
        self._seen: set[str] = set()

    def _poll(self, symbol: str) -> list[dict[str, Any]]:
        response = requests.get(
            f"https://data.alpaca.markets/v2/stocks/{symbol}/trades",
            params={"limit": 100},
            headers={
                "APCA-API-KEY-ID": self.settings.alpaca_key,
                "APCA-API-SECRET-KEY": self.settings.alpaca_secret,
            },
            timeout=10,
        )
        response.raise_for_status()
        output: list[dict[str, Any]] = []
        for item in response.json().get("trades", []):
            record = parse_alpaca_trade(item, symbol)
            if record and record["trade_id"] not in self._seen:
                self._seen.add(record["trade_id"])
                output.append(record)
        return output

    def run(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            for symbol in self.symbols:
                for record in self._poll(symbol):
                    self.kafka.send("equity.trades", record["symbol"], record)
            stop_event.wait(self.interval)
