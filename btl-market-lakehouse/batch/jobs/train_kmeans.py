#!/usr/bin/env python3
"""Cluster assets by return/volatility/volume using Spark MLlib KMeans."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.clustering import KMeans  # noqa: E402
from pyspark.ml.evaluation import ClusteringEvaluator  # noqa: E402
from pyspark.ml.feature import StandardScaler, VectorAssembler  # noqa: E402

from batch.ml_features import symbol_features  # noqa: E402
from batch.spark_session import build_spark  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train asset clusters")
    parser.add_argument("--gold", default="s3a://lake/gold")
    parser.add_argument("--k", type=int, default=3)
    parser.add_argument("--evaluate", action="store_true")
    parser.add_argument("--minio-endpoint", default="")
    parser.add_argument("--access-key", default="")
    parser.add_argument("--secret-key", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark(
        "train-asset-clusters",
        minio_endpoint=args.minio_endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
    )
    daily = spark.read.parquet(f"{args.gold}/daily_bars").toPandas()
    features = symbol_features(daily)
    if features.empty:
        raise SystemExit("No features: run silver_to_gold.py first")

    spark_df = spark.createDataFrame(features)
    assembler = VectorAssembler(
        inputCols=["mean_return", "volatility", "avg_volume"],
        outputCol="raw_features",
    )
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withMean=True,
        withStd=True,
    )
    kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=args.k, seed=42)
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    model = pipeline.fit(spark_df)
    predictions = model.transform(spark_df).select(
        "symbol", "mean_return", "volatility", "avg_volume", "cluster"
    )
    predictions.write.mode("overwrite").parquet(f"{args.gold}/asset_clusters")

    if args.evaluate:
        evaluator = ClusteringEvaluator(
            featuresCol="features", predictionCol="cluster", metricName="silhouette"
        )
        print(f"silhouette={evaluator.evaluate(model.transform(spark_df)):.4f}")

    print(f"clusters written to {args.gold}/asset_clusters")


if __name__ == "__main__":
    main()
