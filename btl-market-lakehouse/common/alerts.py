"""Rule-based alert detection shared by the streaming layer and the demo."""

from __future__ import annotations

import hashlib
from typing import Any

import pandas as pd


def _alert_id(symbol: str, alert_time: str, alert_type: str) -> str:
    # Deterministic per (symbol, bar, type): re-processing the same bar after a restart
    # upserts the same Cassandra row instead of creating a duplicate alert.
    raw = f"{symbol}|{alert_time}|{alert_type}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def make_alert(
    symbol: str,
    alert_time: Any,
    alert_type: str,
    severity: str,
    price: float,
    message: str,
    *,
    asset_class: str | None = None,
    zscore: float | None = None,
    volume_zscore: float | None = None,
) -> dict[str, Any]:
    """Build one record of the ``alerts`` topic / ``alerts_by_day`` table."""
    iso_time = _iso(alert_time)
    record: dict[str, Any] = {
        "alert_id": _alert_id(symbol, iso_time, alert_type),
        "symbol": symbol,
        "alert_time": iso_time,
        "day": iso_time[:10],
        "alert_type": alert_type,
        "severity": severity,
        "price": float(price),
        "zscore": None if zscore is None else round(float(zscore), 4),
        "volume_zscore": None if volume_zscore is None else round(float(volume_zscore), 4),
        "message": message,
    }
    if asset_class:
        record["asset_class"] = asset_class
    return record


def _iso(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def detect_alerts(
    bars: pd.DataFrame,
    z_threshold: float = 3.0,
    volume_z_threshold: float = 3.0,
) -> list[dict[str, Any]]:
    """Detect price and volume anomalies from indicator-enriched OHLCV bars."""
    alerts: list[dict[str, Any]] = []
    if bars.empty:
        return alerts

    for _, row in bars.iterrows():
        symbol = str(row["symbol"])
        price = float(row["close"])
        asset_class = row.get("asset_class")
        asset_class = str(asset_class) if pd.notna(asset_class) else None

        zscore = row.get("zscore_30m")
        if pd.notna(zscore) and abs(float(zscore)) >= z_threshold:
            severity = "high" if abs(float(zscore)) >= 5 else "medium"
            alerts.append(
                make_alert(
                    symbol,
                    row["window_start"],
                    "PRICE_SPIKE",
                    severity,
                    price,
                    f"{symbol} price z-score {float(zscore):.2f} vs previous 30 bars",
                    asset_class=asset_class,
                    zscore=float(zscore),
                )
            )

        volume_z = row.get("volume_zscore_30m")
        if pd.notna(volume_z) and float(volume_z) >= volume_z_threshold:
            alerts.append(
                make_alert(
                    symbol,
                    row["window_start"],
                    "VOLUME_SPIKE",
                    "medium",
                    price,
                    f"{symbol} volume z-score {float(volume_z):.2f} vs previous 30 bars",
                    asset_class=asset_class,
                    volume_zscore=float(volume_z),
                )
            )

    return alerts
