"""Deterministic mock stream, useful when no API key or Kafka is available."""

from __future__ import annotations

import threading
from typing import Any

from common.contracts import synthetic_trades, topic_for

from ..base import BaseProducer


class MockProducer(BaseProducer):
    def __init__(
        self,
        kafka: Any,
        rows: int = 500,
        interval: float = 0.02,
        loop: bool = False,
        seed: int = 42,
    ) -> None:
        super().__init__(kafka)
        self.rows = rows
        self.interval = interval
        self.loop = loop
        self.seed = seed

    def run(self, stop_event: threading.Event) -> None:
        records = [
            record
            for record in synthetic_trades(rows=self.rows, seed=self.seed)
            if record["asset_class"] != "rates"
        ]
        while not stop_event.is_set():
            for record in records:
                topic = topic_for(record["asset_class"], "trades")
                if not self.emit(topic, record["symbol"], record, stop_event, self.interval):
                    return
            if not self.loop:
                return
