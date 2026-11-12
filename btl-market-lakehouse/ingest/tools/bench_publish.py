#!/usr/bin/env python3
"""Upper bound of one producer pod: records/s through the publish path, Kafka excluded.

Runs the synthetic market through :meth:`BaseProducer.publish` (routing, contract
validation, in-process de-duplication, Prometheus counters) into a sink that discards
records. The result tells whether replay at 100x could be limited by the producer
itself rather than by Kafka or Spark (idea PDF, "Mở rộng: Replay 100x vượt sức xử lý").

Example::

    python -m ingest.tools.bench_publish --rows 200000
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.contracts import synthetic_trades  # noqa: E402
from ingest.base import BaseProducer  # noqa: E402


class NullSink:
    def __init__(self) -> None:
        self.count = 0

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self.count += 1

    def flush(self, timeout: float = 10.0) -> None:
        del timeout


class BenchProducer(BaseProducer):
    source = "bench"


def bench(rows: int, seed: int = 42) -> dict[str, Any]:
    records = synthetic_trades(rows=rows, seed=seed, days=30)
    sink = NullSink()
    producer = BenchProducer(sink)
    started = time.perf_counter()
    for record in records:
        producer.publish(record)
    elapsed = time.perf_counter() - started
    return {
        "records": len(records),
        "sent": sink.count,
        "seconds": round(elapsed, 3),
        "records_per_second": round(len(records) / elapsed, 1) if elapsed else None,
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Publish-path throughput benchmark")
    parser.add_argument("--rows", type=int, default=100_000)
    args = parser.parse_args(argv)
    print(json.dumps(bench(args.rows), indent=2))


if __name__ == "__main__":
    main()
