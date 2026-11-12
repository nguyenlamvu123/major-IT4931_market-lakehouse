#!/usr/bin/env python3
"""Copy gold partitions of the processed days into the Cassandra batch tables.

Call stack:

main
  -> batch.gold.read_gold               gold/<table>/dt=<day>/ with the explicit DDL
  -> batch.cassandra_sink.prepare       contract columns + CQL types (common.cql)
  -> batch.cassandra_sink.write         Spark Cassandra Connector, upsert by primary key

``--dry-run`` stops after ``prepare`` and prints the schema and row count per table, so
the job can be checked without a Cassandra node or the connector jar.

Examples::

    python batch/jobs/cassandra_sink.py --lake s3a://lake --dt 2026-11-09
    python batch/jobs/cassandra_sink.py --lake file:///tmp/lake --dt 2026-11-09 --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from batch.cassandra_sink import SINK_TABLES, CassandraSettings, prepare, write  # noqa: E402
from batch.gold import read_gold  # noqa: E402
from batch.lake import Lake, add_day_arguments, days_from_args  # noqa: E402
from batch.spark_session import build_spark  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold to Cassandra")
    add_day_arguments(parser)
    parser.add_argument("--tables", default=",".join(SINK_TABLES))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    days = days_from_args(args)
    tables = [name for name in args.tables.split(",") if name]
    unknown = sorted(set(tables) - set(SINK_TABLES))
    if unknown:
        raise SystemExit(f"unknown sink tables: {unknown}")

    settings = CassandraSettings.from_env()
    spark = build_spark(
        "gold-to-cassandra",
        master=args.master,
        extra_conf={} if args.dry_run else settings.spark_conf(),
    )
    lake = Lake(args.lake)
    for table in tables:
        frame = read_gold(spark, lake, table, days[0], days[-1])
        if args.dry_run:
            prepared = prepare(frame, table)
            print(f"[dry-run] {table}: {prepared.count()} rows -> {settings.keyspace}.{table}")
            prepared.printSchema()
            continue
        write(frame, table, settings)
        print(f"{table}: written to {settings.keyspace}.{table}")


if __name__ == "__main__":
    main()
