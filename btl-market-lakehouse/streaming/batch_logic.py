"""Pure pandas enrichment used inside Spark foreachBatch (unit-testable)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from common.alerts import detect_alerts
from common.indicators import add_indicators

from .columns import ALERT_COLUMNS, BAR_COLUMNS


def enrich_batch(batch: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if batch is None or batch.empty:
        return pd.DataFrame(columns=BAR_COLUMNS), []

    enriched = add_indicators(batch)
    for column in BAR_COLUMNS:
        if column not in enriched.columns:
            enriched[column] = None
    alerts = detect_alerts(enriched)
    return enriched[BAR_COLUMNS].reset_index(drop=True), alerts


def alerts_frame(alerts: list[dict[str, Any]]) -> pd.DataFrame:
    if not alerts:
        return pd.DataFrame(columns=ALERT_COLUMNS)
    frame = pd.DataFrame(alerts)
    frame["alert_time"] = pd.to_datetime(frame["alert_time"], utc=True)
    for column in ALERT_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    return frame[ALERT_COLUMNS].reset_index(drop=True)
