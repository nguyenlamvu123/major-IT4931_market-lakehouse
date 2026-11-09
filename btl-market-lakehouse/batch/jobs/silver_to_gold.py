#!/usr/bin/env python3
"""Silver trades -> daily bars, correlations, yield curve and ML features."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F  # noqa: E402

from batch.schemas import RATE_SCHEMA  # noqa: E402
from batch.spark_session import build_spark  # noqa: E402
from batch.transforms import build_correlations, build_curve, build_features  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Silver to gold")
    parser.add_argument("--silver", default="s3a://lake/silver/trades")
    parser.add_argument("--gold", default="s3a://lake/gold")
    parser.add_argument("--rates", default="", help="Optional rates JSON path")
    parser.add_argument("--window-days", type=int, default=90)
    parser.add_argument("--minio-endpoint", default="")
    parser.add_argument("--access-key", default="")
    parser.add_argument("--secret-key", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark(
        "silver-to-gold",
        minio_endpoint=args.minio_endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
    )
    silver = spark.read.parquet(args.silver)
    daily = (
        silver.groupBy("symbol", F.to_date("window_start").alias("day"))
        .agg(
            F.expr("min_by(open, window_start)").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.expr("max_by(close, window_start)").alias("close"),
            F.sum("volume").alias("volume"),
            F.expr("max_by(vwap, window_start)").alias("vwap"),
        )
        .orderBy("symbol", "day")
    )
    daily.write.mode("overwrite").parquet(f"{args.gold}/daily_bars")

    daily_pdf = daily.toPandas()
    if not daily_pdf.empty:
        correlations = build_correlations(daily_pdf, window_days=args.window_days)
        if not correlations.empty:
            spark.createDataFrame(correlations).write.mode("overwrite").parquet(
                f"{args.gold}/correlation_matrix"
            )

        features = build_features(daily_pdf)
        if not features.empty:
            spark.createDataFrame(features).write.mode("overwrite").parquet(
                f"{args.gold}/ml_features"
            )

    if args.rates:
        rates = spark.read.schema(RATE_SCHEMA).json(args.rates)
        curve = build_curve(rates.toPandas())
        if not curve.empty:
            spark.createDataFrame(curve).write.mode("overwrite").parquet(
                f"{args.gold}/yield_curve"
            )

    print(f"gold tables written to {args.gold}")


if __name__ == "__main__":
    main()
