#!/usr/bin/env python3
"""Silver -> gold: daily summary, cross-asset correlation, yield curve, ML features.

Call stack:

main
  -> batch.gold.build_gold
       -> batch.silver.read_silver            silver/{trades,bars,yields}, dt pruned to the lookback
       -> batch.gold.build_daily              trades + bars, precedence bar_1d > trades > bar_1m
       -> batch.gold.aligned_changes          SPY calendar, bounded forward-fill, returns/yield changes
       -> batch.gold.correlation_as_of        90-row window per processing day
       -> batch.gold.build_yield_curve        pivot tenors, spreads, inversion flag
       -> batch.gold.build_ml_features        per-symbol features for KMeans
  -> batch.gold.write_gold                    gold/<table>/dt=<day>/

Example::

    python batch/jobs/silver_to_gold.py --lake s3a://lake --dt 2026-11-09
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from batch.gold import build_gold, write_gold  # noqa: E402
from batch.lake import Lake, add_day_arguments, days_from_args  # noqa: E402
from batch.spark_session import build_spark  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Silver to gold")
    add_day_arguments(parser)
    parser.add_argument("--window-days", type=int, default=90)
    parser.add_argument("--lookback-days", type=int, default=150)
    parser.add_argument("--reference-symbol", default="SPY")
    args = parser.parse_args()
    days = days_from_args(args)

    spark = build_spark("silver-to-gold", master=args.master)
    lake = Lake(args.lake)
    tables = build_gold(
        spark,
        lake,
        days,
        lookback_days=args.lookback_days,
        window_days=args.window_days,
        reference_symbol=args.reference_symbol,
    )
    write_gold(lake, tables)
    print(f"gold written for {days[0]}..{days[-1]} under {lake.root}/gold")


if __name__ == "__main__":
    main()
