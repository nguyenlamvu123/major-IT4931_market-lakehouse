"""Spark wrapper for alert records."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from .batch_logic import alerts_frame
from .schemas import ALERT_SCHEMA


def alerts_to_dataframe(spark: SparkSession, alerts: list[dict[str, Any]]) -> DataFrame | None:
    frame = alerts_frame(alerts)
    if frame.empty:
        return None
    return spark.createDataFrame(frame, schema=ALERT_SCHEMA)
