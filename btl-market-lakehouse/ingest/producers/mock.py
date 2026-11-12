"""Deterministic mock feed covering every input topic, for demos without API keys.

Publishes the synthetic market from ``common.contracts``: trades on the four trade
topics, 1-minute bars of the ``stocks.trades`` symbols on ``stocks.bars`` and the daily
curve on ``rates.yields``, interleaved in event-time order like a real session.
"""

from __future__ import annotations

import threading
from typing import Any

from common.contracts import (
    INSTRUMENTS,
    make_bar,
    synthetic_trades,
    synthetic_yields,
)

from ..base import BaseProducer, KafkaSink, now_ms

MINUTE_MS = 60_000


def minute_bars(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """1-minute OHLCV bars (contract ``bar``) of the ``stocks.trades`` symbols.

    Pure Python on purpose: the ingest image does not ship pandas.
    """
    buckets: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for trade in trades:
        if INSTRUMENTS[trade["symbol"]].topic != "stocks.trades":
            continue
        start = trade["event_time"] - trade["event_time"] % MINUTE_MS
        buckets.setdefault((trade["symbol"], start), []).append(trade)
    bars: list[dict[str, Any]] = []
    for (symbol, start), items in sorted(buckets.items(), key=lambda item: item[0][1]):
        items.sort(key=lambda trade: trade["event_time"])
        prices = [trade["price"] for trade in items]
        volume = sum(trade["volume"] for trade in items)
        notional = sum(trade["price"] * trade["volume"] for trade in items)
        bars.append(
            make_bar(
                symbol=symbol,
                asset_class=INSTRUMENTS[symbol].asset_class,
                timeframe="1Min",
                event_time=start,
                open_=prices[0],
                high=max(prices),
                low=min(prices),
                close=prices[-1],
                volume=volume,
                source="demo",
                vwap=notional / volume if volume > 0 else None,
                trade_count=len(items),
            )
        )
    return bars


def mock_records(rows: int, seed: int, days: int) -> list[dict[str, Any]]:
    """All mock records in event-time order (rates first on a day, as published at 00:00)."""
    trades = synthetic_trades(rows=rows, seed=seed, days=days)
    records = trades + minute_bars(trades) + synthetic_yields(days=days, seed=seed)
    return sorted(records, key=lambda record: record["event_time"])


class MockProducer(BaseProducer):
    source = "demo"

    def __init__(
        self,
        kafka: KafkaSink,
        rows: int = 500,
        interval: float = 0.02,
        loop: bool = False,
        seed: int = 42,
        days: int = 5,
    ) -> None:
        super().__init__(kafka)
        self.rows = rows
        self.interval = interval
        self.loop = loop
        self.seed = seed
        self.days = days

    def run(self, stop_event: threading.Event) -> None:
        records = mock_records(self.rows, self.seed, self.days)
        while not stop_event.is_set():
            for record in records:
                stamped = dict(record, ingest_time=now_ms())
                if not self.publish_paced(stamped, stop_event, self.interval):
                    return
            if not self.loop:
                return
