"""Spark wrapper around the pure pandas enrichment."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame

from .batch_logic import enrich_batch
from .schemas import BAR_SCHEMA


def enrich_with_indicators(batch_df: DataFrame) -> tuple[DataFrame | None, list[dict[str, Any]]]:
    enriched_pdf, alerts = enrich_batch(batch_df.toPandas())
    if enriched_pdf.empty:
        return None, alerts
    enriched_df = batch_df.sparkSession.createDataFrame(enriched_pdf, schema=BAR_SCHEMA)
    return enriched_df, alerts
