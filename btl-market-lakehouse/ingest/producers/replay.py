"""Replay recorded market data into Kafka at 1x-100x speed.

Why it exists (PDF A task 3, idea PDF slide "Sự cố về hệ thống"): the NYSE session is
21:30-04:00 Vietnam time, so a daytime demo has no live US data. The replay producer
re-publishes stored sessions (local JSONL, contract samples, or the bronze layer on
MinIO) with the original rhythm compressed by ``speed``.

Pacing model: records are sorted by ``event_time``; record ``i`` is due at
``sum(min(gap_k / speed, max_gap))`` seconds after the replay starts, where ``gap_k`` is
the event-time gap to the previous record. ``max_gap`` caps overnight/weekend gaps so a
two-day file does not stall a demo for hours. Due times are measured from the start of
the pass (not from the previous send), so slow Kafka sends do not accumulate drift.

``--rebase-time`` makes the replay look live to the speed layer: each trade/bar gets the
wall-clock time of its send as ``event_time`` (original kept in
``metadata.original_event_time``), ``source="replay"`` and a fresh id, so the second
``--loop`` pass is not discarded by de-duplication downstream. Yields are daily
observations and are never rebased; ``alerts`` records are outputs of the speed layer
and are skipped.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from common.contracts import bar_id, topic_for_record, trade_id

from ..base import BaseProducer, KafkaSink, now_ms

LOGGER = logging.getLogger(__name__)

MIN_SPEED = 1.0
MAX_SPEED = 100.0
MINUTE_MS = 60_000


def due_offsets(event_times: list[int], speed: float, max_gap: float) -> list[float]:
    """Seconds after replay start at which each record (sorted by event time) is due."""
    if not MIN_SPEED <= speed <= MAX_SPEED:
        raise ValueError(f"speed must be within {MIN_SPEED:g}x..{MAX_SPEED:g}x, got {speed}")
    offsets: list[float] = []
    elapsed = 0.0
    previous: int | None = None
    for event_time in event_times:
        if previous is not None:
            gap = max(0.0, (event_time - previous) / 1000.0 / speed)
            elapsed += min(gap, max_gap)
        offsets.append(elapsed)
        previous = event_time
    return offsets


def rebase_record(record: dict[str, Any], wall_ms: int) -> dict[str, Any]:
    """Make a recorded trade/bar look like it happens at ``wall_ms``.

    Before:
    - {"symbol": "BTCUSDT", "event_time": 1794218403120, "source": "binance",
       "exchange_trade_id": "5001", "trade_id": "<old>", ...}

    After:
    - {"symbol": "BTCUSDT", "event_time": <wall_ms>, "source": "replay",
       "trade_id": "<new>", "metadata": {"original_event_time": 1794218403120,
       "original_source": "binance", "original_id": "<old>",
       "original_exchange_trade_id": "5001"}, ...}

    Bars are aligned to the start of the wall-clock minute. Yields are returned as-is.
    """
    if "tenor" in record:
        return dict(record)
    rebased = dict(record)
    metadata = dict(record.get("metadata") or {})
    metadata["original_event_time"] = record["event_time"]
    metadata["original_source"] = record.get("source")
    rebased["source"] = "replay"
    if "bar_id" in record:
        metadata["original_id"] = record["bar_id"]
        rebased["event_time"] = wall_ms - wall_ms % MINUTE_MS
        rebased["metadata"] = metadata
        rebased["bar_id"] = bar_id(rebased)
        return rebased
    metadata["original_id"] = record.get("trade_id")
    exchange_id = rebased.pop("exchange_trade_id", None)
    if exchange_id is not None:
        metadata["original_exchange_trade_id"] = exchange_id
    rebased.pop("trade_id", None)
    rebased["event_time"] = wall_ms
    rebased["metadata"] = metadata
    # Content hash now includes the new event_time, so each loop gets distinct ids.
    rebased["trade_id"] = trade_id(rebased)
    return rebased


def _parse_lines(lines: Iterable[str]) -> Iterator[dict[str, Any]]:
    for line in lines:
        line = line.strip()
        if line:
            yield json.loads(line)


def _is_record_file(path: Path) -> bool:
    # *.jsonl: contract samples / exports. *.json with "+" in the name: Kafka Connect
    # bronze objects (<topic>+<partition>+<offset>.json). Other *.json files in the same
    # tree (e.g. samples/manifest.json) are metadata, not records.
    return path.suffix == ".jsonl" or (path.suffix == ".json" and "+" in path.name)


def load_local(paths: list[str | Path]) -> list[dict[str, Any]]:
    """Read JSON-lines files; a directory means every record file inside it (recursive)."""
    records: list[dict[str, Any]] = []
    for raw in paths:
        path = Path(raw)
        files = sorted(p for p in path.rglob("*") if _is_record_file(p)) if path.is_dir() else [path]
        for file in files:
            with file.open(encoding="utf-8") as handle:
                records.extend(_parse_lines(handle))
    return records


def iter_s3_records(client: Any, uri: str) -> Iterator[dict[str, Any]]:
    """Yield records from every object under an ``s3://bucket/prefix`` (or ``s3a://``).

    Kafka Connect's S3 sink with ``JsonFormat`` writes one JSON document per line, which
    is what the bronze layer contains.
    """
    parsed = urlparse(uri)
    bucket, prefix = parsed.netloc, parsed.path.lstrip("/")
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in sorted(page.get("Contents", []), key=lambda obj: obj["Key"]):
            body = client.get_object(Bucket=bucket, Key=item["Key"])["Body"].read()
            yield from _parse_lines(body.decode("utf-8").splitlines())


def s3_client(endpoint: str, access_key: str, secret_key: str) -> Any:
    import boto3

    # MinIO speaks S3 with path-style addressing; the region is required but unused.
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


class ReplayProducer(BaseProducer):
    source = "replay"
    # Loops re-send records on purpose; rebased records already get fresh ids.
    dedupe_window = 0

    def __init__(
        self,
        kafka: KafkaSink,
        load: Callable[[], list[dict[str, Any]]],
        speed: float = 1.0,
        rebase_time: bool = False,
        loop: bool = False,
        max_gap: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
        wall_ms: Callable[[], int] = now_ms,
    ) -> None:
        super().__init__(kafka)
        due_offsets([], speed, max_gap)  # validate speed before any network work
        self.load = load
        self.speed = speed
        self.rebase_time = rebase_time
        self.loop = loop
        self.max_gap = max_gap
        self.clock = clock
        self.wall_ms = wall_ms
        if loop and not rebase_time:
            LOGGER.warning("--loop without --rebase-time re-sends identical ids; "
                           "downstream de-duplication will drop every pass after the first")

    def _replayable(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        kept = [record for record in records if topic_for_record(record) != "alerts"]
        if len(kept) != len(records):
            LOGGER.info("skipped %d alerts records (speed-layer output)", len(records) - len(kept))
        return sorted(kept, key=lambda record: record["event_time"])

    def run(
        self,
        stop_event: threading.Event,
        wait: Callable[[float], bool] | None = None,
    ) -> None:
        wait = wait or stop_event.wait
        records = self._replayable(self.load())
        if not records:
            LOGGER.warning("nothing to replay")
            return
        offsets = due_offsets([record["event_time"] for record in records], self.speed,
                              self.max_gap)
        while not stop_event.is_set():
            started = self.clock()
            for record, offset in zip(records, offsets, strict=True):
                remaining = started + offset - self.clock()
                if remaining > 0 and wait(remaining):
                    return
                if stop_event.is_set():
                    return
                wall = self.wall_ms()
                outgoing = rebase_record(record, wall) if self.rebase_time else dict(record)
                outgoing["ingest_time"] = wall
                self.publish(outgoing)
            if not self.loop:
                return
