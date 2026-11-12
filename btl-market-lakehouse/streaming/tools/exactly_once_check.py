#!/usr/bin/env python3
"""Check that the speed layer counted every trade exactly once (PDF task B.5).

Expected bars are rebuilt from the raw trades (contract samples or a bronze dump, JSONL)
with the reference aggregation ``common.ohlcv.aggregate_trades``, which de-duplicates
by ``trade_id``. They are compared with what the sink holds now (Cassandra or Parquet).

Cassandra cannot hold two rows for one ``(symbol, day, window_start)``, so a replayed or
double-read batch shows up as an inflated ``trade_count``/``volume``, not as an extra row.
Exit code 1 when anything differs.

    python -m streaming.tools.exactly_once_check --trades 'bronze-dump/*.json' \\
        --cassandra-host market-dc1-service.cassandra.svc
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from common.contracts import TRADE_TOPICS, topic_for_record  # noqa: E402
from common.ohlcv import aggregate_trades  # noqa: E402
from streaming.tools.readers import (  # noqa: E402
    BAR_KEY,
    read_cassandra_table,
    read_parquet_table,
)

FREQ = {"ohlcv_1m": "1min", "ohlcv_5m": "5min"}


def load_trades(patterns: list[str]) -> pd.DataFrame:
    rows: list[dict] = []
    for pattern in patterns:
        for path in sorted(glob.glob(pattern)):
            for line in Path(path).read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                record = json.loads(line)
                # Bronze/sample directories also hold bars, yields and alerts.
                if "trade_id" in record and topic_for_record(record) in TRADE_TOPICS:
                    rows.append(record)
    return pd.DataFrame(rows)


def compare_bars(expected: pd.DataFrame, actual: pd.DataFrame) -> dict:
    """Compare reference bars with sink bars on ``(symbol, window_start)``."""
    expected = expected.assign(window_start=pd.to_datetime(expected["window_start"], utc=True))
    actual = actual.assign(window_start=pd.to_datetime(actual["window_start"], utc=True))
    merged = expected.merge(
        actual, on=BAR_KEY, how="outer", suffixes=("_expected", "_actual"), indicator=True
    )
    both = merged[merged["_merge"] == "both"]
    count_diff = both[both["trade_count_expected"] != both["trade_count_actual"]]
    volume_diff = both[
        (both["volume_expected"] - both["volume_actual"]).abs()
        > 1e-9 * both["volume_expected"].abs().clip(lower=1)
    ]

    def keys(frame: pd.DataFrame) -> list[str]:
        return [f"{row.symbol}@{row.window_start.isoformat()}" for row in frame.itertuples()]

    report = {
        "expected_bars": int(len(expected)),
        "actual_bars": int(len(actual)),
        "missing_in_sink": keys(merged[merged["_merge"] == "left_only"]),
        "unexpected_in_sink": keys(merged[merged["_merge"] == "right_only"]),
        "trade_count_mismatch": keys(count_diff),
        "volume_mismatch": keys(volume_diff),
    }
    report["ok"] = not any(
        report[name]
        for name in (
            "missing_in_sink",
            "unexpected_in_sink",
            "trade_count_mismatch",
            "volume_mismatch",
        )
    )
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--trades", action="append", required=True, help="JSONL glob (repeatable)")
    parser.add_argument("--table", default="ohlcv_1m", choices=sorted(FREQ))
    parser.add_argument("--parquet-root", help="read ParquetSink output instead of Cassandra")
    parser.add_argument("--cassandra-host", default="127.0.0.1")
    parser.add_argument("--cassandra-port", type=int, default=9042)
    parser.add_argument("--keyspace", default="market")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> dict:
    args = parse_args(argv)
    trades = load_trades(args.trades)
    expected = aggregate_trades(trades, FREQ[args.table])
    if args.parquet_root:
        actual = read_parquet_table(args.parquet_root, args.table)
    else:
        days = sorted({str(day.date()) for day in expected["window_start"]})
        actual = read_cassandra_table(
            [args.cassandra_host],
            args.cassandra_port,
            args.keyspace,
            args.table,
            sorted(expected["symbol"].unique()),
            days,
        )
    report = compare_bars(expected, actual)
    print(json.dumps(report, indent=2))
    if not report["ok"]:
        raise SystemExit(1)
    return report


if __name__ == "__main__":
    main()
