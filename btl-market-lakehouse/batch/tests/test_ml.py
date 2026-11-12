"""KMeans asset clustering on the synthetic market (Spark MLlib)."""

from __future__ import annotations

from datetime import date

import pytest

# NOTICE:
# pyspark.ml in PySpark 3.5 imports distutils (pyspark/ml/image.py), which Python 3.12
# removed; it only works when setuptools provides the distutils shim. Skip instead of
# failing on such interpreters (the btl/batch image runs Python 3.10, where it works).
# Removal condition: PySpark >= 4.0.
pytest.importorskip("pyspark.ml", reason="pyspark.ml needs distutils (setuptools) on Python 3.12")

PROCESSING_DAY = date(2026, 11, 25)


def test_bond_etfs_cluster_apart_from_crypto(spark, silver_lake) -> None:
    from batch.cassandra_sink import prepare
    from batch.gold import build_gold
    from batch.ml import train_clusters

    features = build_gold(spark, silver_lake, [PROCESSING_DAY])["ml_features"]
    clusters = train_clusters(features, k=3)
    rows = {row["symbol"]: row for row in clusters.collect()}

    assert rows["TLT"]["cluster"] == rows["IEF"]["cluster"]
    assert rows["BTCUSDT"]["cluster"] == rows["ETHUSDT"]["cluster"]
    assert rows["TLT"]["cluster"] != rows["BTCUSDT"]["cluster"]
    assert {row["model"] for row in rows.values()} == {"kmeans"}
    assert {row["dt"] for row in rows.values()} == {PROCESSING_DAY}
    silhouette = next(iter(rows.values()))["silhouette"]
    assert 0 < silhouette <= 1
    # The result fits the Cassandra contract table written by the sink.
    assert prepare(clusters, "asset_clusters").count() == len(rows)


def test_symbols_without_features_are_left_out(spark) -> None:
    from batch.ml import train_clusters

    rows = [
        (PROCESSING_DAY, PROCESSING_DAY, f"S{index}", "equity", 0.001 * index, 0.01 * index,
         None if index == 0 else -0.1 * index, 100.0, 60)
        for index in range(5)
    ]
    features = spark.createDataFrame(
        rows,
        "dt date, as_of date, symbol string, asset_class string, mean_return double, "
        "volatility double, corr_us10y double, avg_volume double, n_days int",
    )
    clusters = train_clusters(features, k=3)
    assert {row["symbol"] for row in clusters.collect()} == {"S1", "S2", "S3", "S4"}
