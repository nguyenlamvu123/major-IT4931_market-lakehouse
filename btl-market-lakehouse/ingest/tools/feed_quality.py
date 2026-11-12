#!/usr/bin/env python3
"""Duplicate, out-of-order and late rates of a feed sample (report numbers for role A).

The idea PDF ("Lessons learned 1/3", row "Thu thập dữ liệu") asks for "% trùng, % đến
muộn". Definitions used here:

- duplicate: a record whose ``trade_id``/``bar_id`` already appeared in the sample;
- out of order: ``event_time`` earlier than the previous record of the same symbol in
  arrival order (what the speed layer's watermark must absorb);
- late: ``ingest_time - event_time`` above ``late_after_ms`` (default 30 s, the PDF's
  watermark example). Such an event may miss its 1-minute candle in the speed layer and
  is only counted by the batch layer.

Reads JSON lines (a Kafka dump or a bronze file) so it runs without a cluster::

    python -m ingest.tools.feed_quality bronze/crypto.trades/dt=2026-11-09/*.json
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any


def _percentile(sorted_values: list[int], fraction: float) -> float | None:
    if not sorted_values:
        return None
    # Nearest-rank percentile: simple, exact on integers, no interpolation surprises.
    rank = max(1, math.ceil(fraction * len(sorted_values)))
    return float(sorted_values[rank - 1])


def feed_quality(records: Iterable[dict[str, Any]], late_after_ms: int = 30_000) -> dict[str, Any]:
    """Quality summary of records in arrival order."""
    seen: set[str] = set()
    last_event: dict[str, int] = {}
    lags: list[int] = []
    total = duplicates = out_of_order = late = 0
    for record in records:
        total += 1
        record_id = record.get("trade_id") or record.get("bar_id")
        if record_id:
            if record_id in seen:
                duplicates += 1
            seen.add(record_id)
        symbol = str(record.get("symbol") or record.get("tenor") or "")
        event_time = int(record["event_time"])
        if symbol in last_event and event_time < last_event[symbol]:
            out_of_order += 1
        last_event[symbol] = max(event_time, last_event.get(symbol, event_time))
        if record.get("ingest_time") is not None:
            lag = int(record["ingest_time"]) - event_time
            lags.append(lag)
            if lag > late_after_ms:
                late += 1
    lags.sort()

    def pct(count: int) -> float:
        return round(100.0 * count / total, 3) if total else 0.0

    return {
        "records": total,
        "duplicate_pct": pct(duplicates),
        "out_of_order_pct": pct(out_of_order),
        "late_pct": pct(late),
        "late_after_ms": late_after_ms,
        "ingest_lag_p50_ms": _percentile(lags, 0.50),
        "ingest_lag_p95_ms": _percentile(lags, 0.95),
        "ingest_lag_max_ms": float(lags[-1]) if lags else None,
    }


def _read(paths: list[str]) -> Iterable[dict[str, Any]]:
    for raw in paths:
        with Path(raw).open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Feed duplicate/late/out-of-order rates")
    parser.add_argument("files", nargs="+", help="JSON-lines files in arrival order")
    parser.add_argument("--late-after-ms", type=int, default=30_000)
    args = parser.parse_args(argv)
    json.dump(feed_quality(_read(args.files), args.late_after_ms), sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()
