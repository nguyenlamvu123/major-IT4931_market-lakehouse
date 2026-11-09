"""Replay a JSONL file into Kafka with optional pacing."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from common.contracts import topic_for
from common.io_utils import read_jsonl

from ..base import BaseProducer


def topic_for_record(record: dict[str, Any]) -> str:
    if "tenor" in record and "yield" in record:
        return "rates.ticks"
    return topic_for(str(record.get("asset_class", "equity")), "trades")


class ReplayProducer(BaseProducer):
    def __init__(
        self,
        kafka: Any,
        path: str | Path,
        interval: float = 0.0,
        loop: bool = False,
    ) -> None:
        super().__init__(kafka)
        self.path = Path(path)
        self.interval = interval
        self.loop = loop

    def run(self, stop_event: threading.Event) -> None:
        records = read_jsonl(self.path)
        while not stop_event.is_set():
            for record in records:
                key = str(record.get("symbol") or record.get("tenor") or "unknown")
                if not self.emit(
                    topic_for_record(record), key, record, stop_event, self.interval
                ):
                    return
            if not self.loop:
                return
