#!/usr/bin/env python3
"""Quality gate between silver and gold; exits 1 when a rule with severity "error" fails.

Call stack:

main
  -> batch.quality.collect_metrics     Spark aggregations over silver/<table>/dt=<day> + _stats
  -> batch.quality.evaluate            pure rules -> findings (error / warning)
  -> write report                      gold/_quality/dt=<day>/part-*.txt (one JSON document)

In the nightly DAG this task sits between bronze_to_silver and silver_to_gold, so a
failing check stops gold (and Cassandra) from being rebuilt from bad silver data.

Example::

    python batch/jobs/quality_checks.py --lake s3a://lake --dt 2026-11-09
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from batch.lake import Lake, add_day_arguments, days_from_args  # noqa: E402
from batch.quality import collect_metrics, evaluate  # noqa: E402
from batch.spark_session import build_spark  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Silver quality checks")
    add_day_arguments(parser)
    args = parser.parse_args()
    days = days_from_args(args)

    spark = build_spark("quality-checks", master=args.master)
    lake = Lake(args.lake)
    failed: list[str] = []
    for day in days:
        report = evaluate(day, collect_metrics(spark, lake, day))
        document = report.to_json()
        (
            spark.createDataFrame([(document,)], "value string")
            .coalesce(1)
            .write.mode("overwrite")
            .text(f"{lake.gold('_quality')}/dt={day.isoformat()}")
        )
        for finding in report.findings:
            print(f"[{finding.severity}] {day} {finding.dataset} {finding.rule}: "
                  f"{finding.message} ({finding.value})")
        if not report.passed:
            failed.append(day.isoformat())
    if failed:
        raise SystemExit(f"quality checks failed for {failed}")
    print(f"quality checks passed for {days[0]}..{days[-1]}")


if __name__ == "__main__":
    main()
