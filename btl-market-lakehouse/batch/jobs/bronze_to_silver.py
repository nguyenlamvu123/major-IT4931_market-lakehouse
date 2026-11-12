#!/usr/bin/env python3
"""Bronze JSON -> silver Parquet for one UTC day (or a backfill range).

Call stack:

main
  -> batch.silver.build_silver          read bronze/<topic>/dt=<day>/, validate, de-duplicate
       -> batch.silver.read_bronze      one read per topic (never the bronze root)
       -> batch.silver.check_*          reject_reason per row
       -> batch.silver.dedupe           deterministic row per key
  -> batch.silver.write_silver          silver/{trades,bars,yields,alerts}/dt=…, silver/_stats/dt=…

Examples::

    python batch/jobs/bronze_to_silver.py --lake s3a://lake --dt 2026-11-09
    python batch/jobs/bronze_to_silver.py --lake file:///tmp/lake --start 2026-09-01 \
        --end 2026-11-30 --master 'local[2]'
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from batch.lake import Lake, add_day_arguments, days_from_args  # noqa: E402
from batch.silver import build_silver, write_silver  # noqa: E402
from batch.spark_session import build_spark  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze to silver")
    add_day_arguments(parser)
    args = parser.parse_args()
    days = days_from_args(args)

    spark = build_spark("bronze-to-silver", master=args.master)
    lake = Lake(args.lake)
    tables, stats = build_silver(spark, lake, days)
    write_silver(spark, lake, tables, stats)
    for item in stats:
        print(json.dumps(asdict(item), ensure_ascii=False))
    print(f"silver written for {days[0]}..{days[-1]} under {lake.root}/silver")


if __name__ == "__main__":
    main()
