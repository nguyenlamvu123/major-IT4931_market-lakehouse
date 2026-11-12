"""Replay a JSONL file of contract records into Kafka with fixed pacing."""

from __future__ import annotations

import threading
from pathlib import Path

from common.io_utils import read_jsonl

from ..base import BaseProducer, KafkaSink


class ReplayProducer(BaseProducer):
    source = "replay"

    def __init__(
        self,
        kafka: KafkaSink,
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
                # Topic and key come from the contract (topic_for_record), so one file
                # may mix trades, bars and yields.
                if not self.publish_paced(record, stop_event, self.interval):
                    return
            if not self.loop:
                return
