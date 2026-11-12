"""Data-lake layout (contracts/paths.yaml) and the day-range arguments every job takes.

Layout, relative to the lake root (``s3a://lake`` on the cluster, ``file:///...`` in
tests and local runs):

- ``bronze/<topic>/dt=YYYY-MM-DD/``  JSON lines written by the Kafka Connect S3 sink (A)
- ``silver/<table>/dt=YYYY-MM-DD/``  Parquet, cleaned and de-duplicated (C)
- ``gold/<table>/dt=YYYY-MM-DD/``    Parquet, analytics tables (C)

``dt`` is always the UTC date of the event (bronze/silver) or the processing day (gold).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # keep this module importable without pyspark (pure quality rules)
    from pyspark.sql import SparkSession

DEFAULT_LAKE = "s3a://lake"

SILVER_TABLES = ("trades", "bars", "yields", "alerts")

#: FRED and the Treasury publish a day's yields on the next US business day, after the
#: nightly run for that day has already happened, and FRED revises recent values. Every
#: run therefore re-reads rates.yields for this many trailing days, so late or revised
#: observations still reach silver, the gold yield curve and Cassandra.
YIELD_LATENCY_DAYS = 7
GOLD_TABLES = (
    "daily_summary",
    "correlation_matrix",
    "yield_curve",
    "ml_features",
    "asset_clusters",
)


@dataclass(frozen=True)
class Lake:
    root: str = DEFAULT_LAKE

    def _join(self, *parts: str) -> str:
        return "/".join([self.root.rstrip("/"), *parts])

    def bronze(self, topic: str, day: date | None = None) -> str:
        if day is None:
            return self._join("bronze", topic)
        return self._join("bronze", topic, f"dt={day.isoformat()}")

    def silver(self, table: str) -> str:
        if table not in SILVER_TABLES and not table.startswith("_"):
            raise ValueError(f"unknown silver table {table!r}")
        return self._join("silver", table)

    def gold(self, table: str) -> str:
        if table not in GOLD_TABLES and not table.startswith("_"):
            raise ValueError(f"unknown gold table {table!r}")
        return self._join("gold", table)


def day_range(start: date, end: date) -> list[date]:
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def add_day_arguments(parser: argparse.ArgumentParser) -> None:
    """``--dt`` (one day, what Airflow passes as ``{{ ds }}``) or ``--start/--end``."""
    parser.add_argument("--lake", default=DEFAULT_LAKE, help="lake root, e.g. s3a://lake")
    parser.add_argument("--dt", type=date.fromisoformat, help="UTC day to process")
    parser.add_argument("--start", type=date.fromisoformat, help="first UTC day (backfill)")
    parser.add_argument("--end", type=date.fromisoformat, help="last UTC day (backfill)")
    parser.add_argument("--master", default=None, help="Spark master, e.g. local[2]")


def days_from_args(args: argparse.Namespace) -> list[date]:
    if args.dt and (args.start or args.end):
        raise SystemExit("use either --dt or --start/--end, not both")
    if args.dt:
        return [args.dt]
    if args.start and args.end:
        return day_range(args.start, args.end)
    raise SystemExit("missing --dt or --start/--end")


def existing_paths(spark: SparkSession, paths: list[str]) -> list[str]:
    """Keep the paths that exist, using the Hadoop FileSystem of each URI.

    Spark fails the whole read when one listed path is missing, and a missing bronze day
    is normal (no stock trades on Sunday, a producer that was down). Checking through
    Hadoop works the same for ``s3a://`` and ``file://``.
    """
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    kept: list[str] = []
    for path in paths:
        hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
        if hadoop_path.getFileSystem(conf).exists(hadoop_path):
            kept.append(path)
    return kept
