"""Pure pandas shaping of indicators and alerts for the Cassandra tables (no Spark)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from common.alerts import detect_alerts
from common.indicators import add_indicators

from .columns import ALERT_COLUMNS, INDICATOR_COLUMNS


def indicator_rows(enriched: pd.DataFrame, updated_at: pd.Timestamp) -> pd.DataFrame:
    """Newest bar of each symbol, shaped as ``indicators_latest`` rows."""
    if enriched.empty:
        return pd.DataFrame(columns=INDICATOR_COLUMNS)
    latest = (
        enriched.sort_values(["symbol", "window_start"])
        .groupby("symbol", as_index=False)
        .tail(1)
        .copy()
    )
    latest["updated_at"] = updated_at
    for column in INDICATOR_COLUMNS:
        if column not in latest.columns:
            latest[column] = None
    return latest[INDICATOR_COLUMNS].reset_index(drop=True)


def alert_rows(alerts: list[dict[str, Any]]) -> pd.DataFrame:
    """``common.alerts`` records -> ``alerts_by_day`` rows (typed day/alert_time)."""
    if not alerts:
        return pd.DataFrame(columns=ALERT_COLUMNS)
    frame = pd.DataFrame(alerts)
    frame["alert_time"] = pd.to_datetime(frame["alert_time"], utc=True)
    frame["day"] = frame["alert_time"].dt.date
    for column in ALERT_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    return frame[ALERT_COLUMNS].reset_index(drop=True)


def enrich_batch(bars: pd.DataFrame, updated_at: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Indicators + alerts computed on the bars of one micro-batch only."""
    if bars is None or bars.empty:
        return pd.DataFrame(columns=INDICATOR_COLUMNS), pd.DataFrame(columns=ALERT_COLUMNS)
    enriched = add_indicators(bars)
    return indicator_rows(enriched, updated_at), alert_rows(detect_alerts(enriched))
