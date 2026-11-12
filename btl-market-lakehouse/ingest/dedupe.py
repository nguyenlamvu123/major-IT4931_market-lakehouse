"""Bounded memory of recently published record ids."""

from __future__ import annotations

from collections import OrderedDict


class RecentIds:
    """Least-recently-seen set of ids with a fixed capacity.

    Replaces the old unbounded ``set`` in the Alpaca poller, which grew for as long as the
    pod lived. A reconnect only replays the last few seconds of a feed, so remembering
    the newest ``capacity`` ids is enough to drop those re-sent records; older duplicates
    are still removed by the batch layer (silver de-duplicates on ``trade_id``).
    """

    def __init__(self, capacity: int = 50_000) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self.capacity = capacity
        self._ids: OrderedDict[str, None] = OrderedDict()

    def __len__(self) -> int:
        return len(self._ids)

    def seen(self, record_id: str) -> bool:
        """Return True if ``record_id`` was already recorded; record it either way."""
        if record_id in self._ids:
            self._ids.move_to_end(record_id)
            return True
        self._ids[record_id] = None
        if len(self._ids) > self.capacity:
            self._ids.popitem(last=False)
        return False
