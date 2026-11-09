"""Yahoo Finance fallback poller for equities/ETFs."""

from __future__ import annotations

import threading
from typing import Any

from common.contracts import make_trade

from ..base import BaseProducer


class YahooProducer(BaseProducer):
    def __init__(self, kafka: Any, symbols: list[str], interval: float = 60.0) -> None:
        super().__init__(kafka)
        self.symbols = [symbol.upper() for symbol in symbols]
        self.interval = interval

    def _poll(self) -> list[dict[str, Any]]:
        import yfinance as yf

        frame = yf.download(
            tickers=self.symbols,
            period="1d",
            interval="1m",
            progress=False,
            auto_adjust=False,
            group_by="ticker",
            threads=False,
        )
        records: list[dict[str, Any]] = []
        if frame is None or frame.empty:
            return records
        for symbol in self.symbols:
            try:
                row = frame[symbol].dropna().iloc[-1] if len(self.symbols) > 1 else frame.dropna().iloc[-1]
                event_time = int(row.name.timestamp() * 1000)
                records.append(
                    make_trade(
                        symbol=symbol,
                        asset_class="equity",
                        event_time=event_time,
                        price=float(row["Close"]),
                        volume=float(row.get("Volume", 0)),
                        source="yahoo",
                    )
                )
            except (KeyError, IndexError, TypeError, ValueError):
                continue
        return records

    def run(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            try:
                for record in self._poll():
                    self.kafka.send("equity.trades", record["symbol"], record)
            except Exception:  # noqa: BLE001 - keep the poller alive
                pass
            stop_event.wait(self.interval)
