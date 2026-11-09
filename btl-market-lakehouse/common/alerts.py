"""Rule-based alert detection shared by the streaming layer and the demo."""

from __future__ import annotations

import hashlib
from typing import Any

import pandas as pd


def _alert_id(symbol: str, alert_time: str, alert_type: str) -> str:
    raw = f"{symbol}|{alert_time}|{alert_type}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


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
        alert_time = _iso(row["window_start"])
        price = float(row["close"])

        zscore = row.get("zscore_30m")
        if pd.notna(zscore) and abs(float(zscore)) >= z_threshold:
            severity = "high" if abs(float(zscore)) >= 5 else "medium"
            alerts.append(
                {
                    "alert_id": _alert_id(symbol, alert_time, "PRICE_SPIKE"),
                    "symbol": symbol,
                    "alert_time": alert_time,
                    "alert_type": "PRICE_SPIKE",
                    "severity": severity,
                    "price": price,
                    "zscore": round(float(zscore), 4),
                    "message": f"{symbol} price z-score {float(zscore):.2f}",
                }
            )

        volume_z = row.get("volume_zscore_30m")
        if pd.notna(volume_z) and float(volume_z) >= volume_z_threshold:
            alerts.append(
                {
                    "alert_id": _alert_id(symbol, alert_time, "VOLUME_SPIKE"),
                    "symbol": symbol,
                    "alert_time": alert_time,
                    "alert_type": "VOLUME_SPIKE",
                    "severity": "medium",
                    "price": price,
                    "volume_zscore": round(float(volume_z), 4),
                    "message": f"{symbol} volume z-score {float(volume_z):.2f}",
                }
            )

    return alerts
