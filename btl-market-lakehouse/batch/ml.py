"""Asset clustering with Spark MLlib KMeans (PDF task C.5: "phân cụm tài sản").

Input: ``gold/ml_features`` (one row per symbol and processing day, built by
``batch.gold.build_ml_features``). Features:

- ``mean_return``: average daily return over the last 90 own trading days,
- ``volatility``: std of those returns,
- ``corr_us10y``: correlation with 10Y yield changes (bond-like assets are strongly
  negative, crypto and equities are near zero).

The three features live on different scales (returns ~1e-3, correlations ~1), so they
are standardised before KMeans; otherwise volatility alone would decide the clusters.
"""

from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FEATURE_COLUMNS = ["mean_return", "volatility", "corr_us10y"]
MODEL_NAME = "kmeans"
CLUSTER_COLUMNS = [
    "model",
    "as_of",
    "symbol",
    "asset_class",
    "cluster",
    "mean_return",
    "volatility",
    "corr_us10y",
    "silhouette",
    "dt",
]


def train_clusters(features: DataFrame, k: int = 3, seed: int = 42) -> DataFrame:
    """Fit one KMeans model per processing day ``dt`` and return the assignments.

    Symbols with a NULL feature (e.g. ``corr_us10y`` when the pair had fewer than 3
    common days) are left out of that day's model rather than imputed: a made-up
    correlation would pull the symbol into the wrong cluster. ``k`` is capped at the
    number of usable symbols - 1 so the silhouette score stays defined.
    """
    usable = features.dropna(subset=FEATURE_COLUMNS)
    results: list[DataFrame] = []
    for row in usable.select("dt").distinct().orderBy("dt").collect():
        day_features = usable.filter(F.col("dt") == F.lit(row["dt"])).cache()
        symbols = day_features.count()
        if symbols < 3:
            continue
        clusters = min(k, symbols - 1)
        pipeline = Pipeline(
            stages=[
                VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="raw_features"),
                StandardScaler(
                    inputCol="raw_features", outputCol="features", withMean=True, withStd=True
                ),
                KMeans(featuresCol="features", predictionCol="cluster", k=clusters, seed=seed),
            ]
        )
        predictions = pipeline.fit(day_features).transform(day_features)
        silhouette = ClusteringEvaluator(
            featuresCol="features", predictionCol="cluster", metricName="silhouette"
        ).evaluate(predictions)
        results.append(
            predictions.select(
                F.lit(MODEL_NAME).alias("model"),
                "as_of",
                "symbol",
                "asset_class",
                F.col("cluster").cast("int").alias("cluster"),
                *FEATURE_COLUMNS,
                F.lit(float(silhouette)).alias("silhouette"),
                "dt",
            )
        )
    if not results:
        return features.sparkSession.createDataFrame(
            [],
            "model string, as_of date, symbol string, asset_class string, cluster int, "
            "mean_return double, volatility double, corr_us10y double, silhouette double, "
            "dt date",
        )
    combined = results[0]
    for frame in results[1:]:
        combined = combined.unionByName(frame)
    return combined.select(CLUSTER_COLUMNS)
