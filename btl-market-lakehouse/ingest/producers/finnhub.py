"""Finnhub quote poller (small and dependency-light)."""

from __future__ import annotations

import threading
from typing import Any

import requests

from common.contracts import make_trade

from ..base import BaseProducer
from ..config import ApiSettings


def parse_finnhub_quote(payload: dict[str, Any], symbol: str) -> dict[str, Any] | None:
    try:
        price = float(payload["c"])
        event_time = int(payload["t"]) * 1000
    except (KeyError, TypeError, ValueError):
        return None
    if price <= 0 or event_time <= 0:
        return None
    return make_trade(
        symbol=symbol.upper(),
        asset_class="equity",
        event_time=event_time,
        price=price,
        volume=0.0,
        source="finnhub",
        metadata={"quote": payload},
    )


class FinnhubProducer(BaseProducer):
    def __init__(
        self,
        kafka: Any,
        settings: ApiSettings,
        symbols: list[str],
        interval: float = 30.0,
    ) -> None:
        super().__init__(kafka)
        if not settings.finnhub_key:
            raise RuntimeError("FINNHUB_API_KEY is required")
        self.settings = settings
        self.symbols = [symbol.upper() for symbol in symbols]
        self.interval = interval

    def run(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            for symbol in self.symbols:
                response = requests.get(
                    "https://finnhub.io/api/v1/quote",
                    params={"symbol": symbol, "token": self.settings.finnhub_key},
                    timeout=10,
                )
                if response.ok:
                    record = parse_finnhub_quote(response.json(), symbol)
                    if record:
                        self.kafka.send("equity.trades", record["symbol"], record)
            stop_event.wait(self.interval)
