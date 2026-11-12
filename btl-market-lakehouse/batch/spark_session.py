"""SparkSession builder shared by every batch job (local, Docker Compose, Spark Operator)."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def build_spark(
    app_name: str,
    *,
    master: str | None = None,
    shuffle_partitions: int = 8,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Create (or reuse) the SparkSession for a batch job.

    - The session time zone is pinned to UTC. The contract partitions every table by the
      UTC date (``dt``), and ``to_date(timestamp)`` follows the session time zone; a
      driver running on a laptop set to Asia/Ho_Chi_Minh would otherwise shift trades
      between 17:00 and 24:00 UTC into the next day.
    - ``partitionOverwriteMode=dynamic`` makes ``mode("overwrite")`` replace only the
      ``dt`` partitions present in the written DataFrame, so re-running one day is
      idempotent and never wipes other days.
    - MinIO credentials come from the environment (``MINIO_ENDPOINT``,
      ``MINIO_ACCESS_KEY``, ``MINIO_SECRET_KEY``), which the SparkApplication fills from
      the ``minio-credentials`` secret. They are never passed as CLI arguments because
      those show up in ``ps`` output and in the SparkApplication spec.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    )
    if master:
        builder = builder.master(master)

    endpoint = os.getenv("MINIO_ENDPOINT", "")
    if endpoint:
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", ""))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", ""))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(endpoint.startswith("https")).lower(),
            )
        )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
