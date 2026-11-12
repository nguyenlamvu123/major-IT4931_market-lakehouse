"""Replay pacing (1x-100x), time rebasing and bronze/S3 loading, with a fake clock."""

from __future__ import annotations

import io
import json
import threading
from pathlib import Path
from typing import Any

import pytest

from common.contracts import (
    make_bar,
    make_rate,
    make_trade,
    validate_bar,
    validate_trade,
)
from ingest.producers.replay import (
    ReplayProducer,
    due_offsets,
    iter_s3_records,
    load_local,
    rebase_record,
)

SAMPLES = Path(__file__).resolve().parents[2] / "contracts" / "samples"


class FakeKafka:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict[str, Any]]] = []

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self.sent.append((topic, key, value))

    def flush(self, timeout: float = 10.0) -> None:
        del timeout


class FakeTime:
    """Monotonic clock that only advances when the producer waits."""

    def __init__(self) -> None:
        self.now = 100.0
        self.waits: list[float] = []

    def clock(self) -> float:
        return self.now

    def wait(self, seconds: float) -> bool:
        self.waits.append(round(seconds, 6))
        self.now += seconds
        return False


def _trade(event_time: int, exchange_id: int) -> dict[str, Any]:
    return make_trade("BTCUSDT", "crypto", event_time, 92400.0, 0.05, "binance",
                      exchange_trade_id=exchange_id)


def test_due_offsets_compress_by_speed_and_cap_gaps() -> None:
    # 10 s and 20 s gaps at 10x -> 1 s and 2 s; an overnight 17 h gap is capped at 5 s.
    times = [0, 10_000, 30_000, 30_000 + 17 * 3_600_000]
    assert due_offsets(times, speed=10, max_gap=5.0) == [0.0, 1.0, 3.0, 8.0]
    assert due_offsets([0, 2_000], speed=1, max_gap=5.0) == [0.0, 2.0]
    with pytest.raises(ValueError):
        due_offsets(times, speed=0.5, max_gap=5.0)
    with pytest.raises(ValueError):
        due_offsets(times, speed=101, max_gap=5.0)


def test_replay_paces_from_run_start_without_drift() -> None:
    fake = FakeTime()
    records = [_trade(0, 1), _trade(4_000, 2), _trade(6_000, 3)]
    kafka = FakeKafka()
    producer = ReplayProducer(kafka, lambda: list(reversed(records)), speed=2.0,
                              clock=fake.clock, wall_ms=lambda: 1_794_300_000_000)
    producer.run(threading.Event(), wait=fake.wait)
    assert fake.waits == [2.0, 1.0]
    assert [value["exchange_trade_id"] for _, _, value in kafka.sent] == ["1", "2", "3"]
    # Without --rebase-time the original event time and source are kept.
    assert kafka.sent[0][2]["event_time"] == 0
    assert kafka.sent[0][2]["source"] == "binance"
    assert kafka.sent[0][2]["ingest_time"] == 1_794_300_000_000


def test_rebase_gives_fresh_ids_and_keeps_originals() -> None:
    original = _trade(1_794_218_403_120, 5001)
    first = rebase_record(original, 1_794_300_000_500)
    second = rebase_record(original, 1_794_300_060_500)
    assert first["event_time"] == 1_794_300_000_500
    assert first["source"] == "replay"
    assert first["metadata"]["original_event_time"] == 1_794_218_403_120
    assert first["metadata"]["original_id"] == original["trade_id"]
    assert first["metadata"]["original_exchange_trade_id"] == "5001"
    assert first["trade_id"] != original["trade_id"]
    assert first["trade_id"] != second["trade_id"]  # next --loop pass is not a duplicate
    assert validate_trade(first) == []

    bar = make_bar("SPY", "equity", "1Min", 1_794_238_200_000, 590, 591, 589, 590.5, 10, "alpaca")
    moved = rebase_record(bar, 1_794_300_012_345)
    assert moved["event_time"] == 1_794_300_000_000  # aligned to the minute
    assert moved["bar_id"] != bar["bar_id"]
    assert validate_bar(moved) == []

    rate = make_rate("DGS10", "2026-11-09", 4.15, "treasury")
    assert rebase_record(rate, 1_794_300_000_000) == rate


def test_loop_with_rebase_sends_distinct_ids_and_skips_alerts() -> None:
    fake = FakeTime()
    wall = {"ms": 1_794_300_000_000}

    def wall_ms() -> int:
        wall["ms"] += 1_000
        return wall["ms"]

    alert = {"alert_id": "a" * 16, "symbol": "BTCUSDT", "alert_type": "PRICE_SPIKE",
             "alert_time": "2026-11-09T10:00:00+00:00", "event_time": 1}
    records = [_trade(0, 1), _trade(1_000, 2), alert]
    stop = threading.Event()
    kafka = FakeKafka()
    producer = ReplayProducer(kafka, lambda: records, speed=100, rebase_time=True, loop=True,
                              clock=fake.clock, wall_ms=wall_ms)

    def wait(seconds: float) -> bool:
        fake.wait(seconds)
        if len(kafka.sent) >= 4:
            stop.set()
            return True
        return False

    producer.run(stop, wait=wait)
    ids = [value["trade_id"] for _, _, value in kafka.sent]
    assert len(ids) >= 4  # at least two full passes of the two trades
    assert len(set(ids)) == len(ids)
    assert {topic for topic, _, _ in kafka.sent} == {"crypto.trades"}


def test_load_local_reads_contract_samples_directory() -> None:
    records = load_local([SAMPLES])
    assert len(records) == 7 * 20
    kafka = FakeKafka()
    fake = FakeTime()
    ReplayProducer(kafka, lambda: records, speed=100, max_gap=0.0,
                   clock=fake.clock).run(threading.Event(), wait=fake.wait)
    topics = {topic for topic, _, _ in kafka.sent}
    assert topics == {"crypto.trades", "stocks.trades", "stocks.bars", "gold.quotes",
                      "fx.dxy", "rates.yields"}
    assert len(kafka.sent) == 6 * 20


class FakeS3:
    def __init__(self, objects: dict[str, str]) -> None:
        self.objects = objects
        self.prefixes: list[str] = []

    def get_paginator(self, name: str) -> FakeS3:
        assert name == "list_objects_v2"
        return self

    def paginate(self, Bucket: str, Prefix: str) -> list[dict[str, Any]]:  # noqa: N803
        assert Bucket == "lake"
        self.prefixes.append(Prefix)
        keys = [key for key in self.objects if key.startswith(Prefix)]
        return [{"Contents": [{"Key": key} for key in keys[:1]]},
                {"Contents": [{"Key": key} for key in keys[1:]]}]

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        return {"Body": io.BytesIO(self.objects[Key].encode("utf-8"))}


def test_iter_s3_records_reads_bronze_json_lines() -> None:
    prefix = "bronze/crypto.trades/dt=2026-11-09/"
    lines = [json.dumps(_trade(index, index)) for index in range(3)]
    client = FakeS3({
        prefix + "crypto.trades+0+0000000000.json": "\n".join(lines[:2]) + "\n",
        prefix + "crypto.trades+1+0000000000.json": lines[2] + "\n",
        "bronze/stocks.trades/dt=2026-11-09/x.json": lines[0],
    })
    records = list(iter_s3_records(client, "s3a://lake/" + prefix))
    assert client.prefixes == [prefix]
    assert [record["exchange_trade_id"] for record in records] == ["0", "1", "2"]
