"""SparkSession builder with optional MinIO/S3A configuration."""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark(
    app_name: str = "market-batch",
    minio_endpoint: str = "",
    access_key: str = "",
    secret_key: str = "",
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if minio_endpoint:
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    return builder.getOrCreate()
