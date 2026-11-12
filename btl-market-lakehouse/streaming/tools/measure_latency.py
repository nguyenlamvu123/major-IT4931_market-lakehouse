#!/usr/bin/env python3
"""Measure trade -> Cassandra latency of the speed layer (PDF task B.6).

For every bar, latency = ``updated_at - last_event_time``: the time between the newest
trade of the bar and the upsert that made it visible. Prints p50/p95/max as JSON and can
append a dated row to the metrics log so the report never has to re-measure.

    python -m streaming.tools.measure_latency --day 2026-11-09 \\
        --cassandra-host market-dc1-service.cassandra.svc --append-metrics docs/metrics/B-speed.md
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from common.contracts import INSTRUMENTS  # noqa: E402
from streaming.metrics import latency_seconds, latency_stats  # noqa: E402
from streaming.tools.readers import read_cassandra_table, read_parquet_table  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--table", default="ohlcv_1m", choices=["ohlcv_1m", "ohlcv_5m"])
    parser.add_argument("--day", action="append", help="UTC day YYYY-MM-DD (repeatable)")
    parser.add_argument("--symbols", default=",".join(INSTRUMENTS))
    parser.add_argument("--parquet-root", help="read ParquetSink output instead of Cassandra")
    parser.add_argument("--cassandra-host", default="127.0.0.1")
    parser.add_argument("--cassandra-port", type=int, default=9042)
    parser.add_argument("--keyspace", default="market")
    parser.add_argument("--append-metrics", help="markdown file to append a result row to")
    parser.add_argument("--note", default="", help="configuration note for the metrics row")
    return parser.parse_args(argv)


def metrics_row(table: str, stats: dict, note: str, now: datetime) -> str:
    """One markdown table row: date | measurement | configuration | result."""
    result = f"n={stats['count']} p50={stats['p50']}s p95={stats['p95']}s max={stats['max']}s"
    return f"| {now:%Y-%m-%d %H:%M} UTC | latency trade->{table} | {note or '-'} | {result} |\n"


def main(argv: list[str] | None = None) -> dict:
    args = parse_args(argv)
    days = args.day or [datetime.now(timezone.utc).strftime("%Y-%m-%d")]
    symbols = [symbol for symbol in args.symbols.split(",") if symbol]
    if args.parquet_root:
        bars = read_parquet_table(args.parquet_root, args.table)
    else:
        bars = read_cassandra_table(
            [args.cassandra_host], args.cassandra_port, args.keyspace, args.table, symbols, days
        )
    stats = latency_stats(latency_seconds(bars))
    print(json.dumps({"table": args.table, "days": days, **stats}, indent=2))
    if args.append_metrics:
        with open(args.append_metrics, "a", encoding="utf-8") as handle:
            handle.write(metrics_row(args.table, stats, args.note, datetime.now(timezone.utc)))
    return stats


if __name__ == "__main__":
    main()
