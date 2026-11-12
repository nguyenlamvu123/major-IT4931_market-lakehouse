#!/usr/bin/env python3
"""Cluster assets with Spark MLlib KMeans and publish the result (gold + Cassandra).

Call stack:

main
  -> batch.gold.read_gold            gold/ml_features/dt=<day>/
  -> batch.ml.train_clusters         StandardScaler + KMeans per day, silhouette score
  -> write gold/asset_clusters/dt=<day>/
  -> batch.cassandra_sink.write      market.asset_clusters (skipped with --dry-run)

Runs after cassandra_sink in the nightly DAG, so it publishes its own table.

Example::

    python batch/jobs/train_kmeans.py --lake s3a://lake --dt 2026-11-09 --k 3
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from batch.cassandra_sink import CassandraSettings, write  # noqa: E402
from batch.gold import read_gold  # noqa: E402
from batch.lake import Lake, add_day_arguments, days_from_args  # noqa: E402
from batch.ml import train_clusters  # noqa: E402
from batch.spark_session import build_spark  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Train asset clusters (KMeans)")
    add_day_arguments(parser)
    parser.add_argument("--k", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true", help="skip the Cassandra write")
    args = parser.parse_args()
    days = days_from_args(args)

    settings = CassandraSettings.from_env()
    spark = build_spark(
        "train-asset-clusters",
        master=args.master,
        extra_conf={} if args.dry_run else settings.spark_conf(),
    )
    lake = Lake(args.lake)
    features = read_gold(spark, lake, "ml_features", days[0], days[-1])
    clusters = train_clusters(features, k=args.k).cache()
    if clusters.limit(1).count() == 0:
        raise SystemExit("no usable ml_features rows: run silver_to_gold first")

    clusters.write.mode("overwrite").partitionBy("dt").parquet(lake.gold("asset_clusters"))
    for row in clusters.select("dt", "silhouette").distinct().orderBy("dt").collect():
        print(f"dt={row['dt']} silhouette={row['silhouette']:.4f}")
    if not args.dry_run:
        write(clusters, "asset_clusters", settings)
        print(f"asset_clusters written to {settings.keyspace}.asset_clusters")


if __name__ == "__main__":
    main()
