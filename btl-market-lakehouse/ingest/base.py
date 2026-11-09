"""Common producer lifecycle helpers."""

from __future__ import annotations

import logging
import threading
from typing import Any

LOGGER = logging.getLogger(__name__)


class BaseProducer:
    def __init__(self, kafka: Any) -> None:
        self.kafka = kafka

    def run(self, stop_event: threading.Event) -> None:
        raise NotImplementedError

    def close(self) -> None:
        self.kafka.flush()

    def emit(
        self,
        topic: str,
        key: str,
        value: dict[str, Any],
        stop_event: threading.Event,
        interval: float = 0.0,
    ) -> bool:
        """Send one record; return False when a stop was requested."""
        self.kafka.send(topic, key, value)
        if interval > 0 and stop_event.wait(interval):
            return False
        return not stop_event.is_set()
