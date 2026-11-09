#!/usr/bin/env python3
"""Bronze JSON -> cleaned, deduplicated silver Parquet."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F  # noqa: E402

from batch.schemas import TRADE_SCHEMA  # noqa: E402
from batch.spark_session import build_spark  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze to silver trades")
    parser.add_argument("--bronze", default="s3a://lake/bronze")
    parser.add_argument("--silver", default="s3a://lake/silver/trades")
    parser.add_argument("--mode", choices=["append", "overwrite"], default="overwrite")
    parser.add_argument("--minio-endpoint", default="")
    parser.add_argument("--access-key", default="")
    parser.add_argument("--secret-key", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark(
        "bronze-to-silver",
        minio_endpoint=args.minio_endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
    )
    bronze = spark.read.schema(TRADE_SCHEMA).json(args.bronze)
    silver = (
        bronze.filter(F.col("symbol").isNotNull())
        .filter(F.col("price") > 0)
        .filter(F.col("volume") >= 0)
        .withColumn("event_time_ts", (F.col("event_time") / 1000).cast("timestamp"))
        .withColumn("event_date", F.to_date("event_time_ts"))
        .withColumn("event_hour", F.hour("event_time_ts"))
        .dropDuplicates(["trade_id"])
    )
    (
        silver.write.mode(args.mode)
        .partitionBy("asset_class", "event_date")
        .parquet(args.silver)
    )
    print(f"silver rows written to {args.silver}")


if __name__ == "__main__":
    main()
