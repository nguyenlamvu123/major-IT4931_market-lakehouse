"""Deterministic demo repository so the API works without Cassandra."""

from __future__ import annotations

from typing import Any

import pandas as pd

from common.alerts import detect_alerts
from common.contracts import synthetic_trades
from common.indicators import add_indicators
from common.io_utils import df_to_records
from common.ohlcv import aggregate_trades


class DemoRepository:
    def __init__(self) -> None:
        self._bars: pd.DataFrame | None = None
        self._alerts: list[dict[str, Any]] = []

    def _load(self) -> None:
        if self._bars is not None:
            return
        trades = pd.DataFrame(synthetic_trades(rows=2_400, seed=42))
        trades = trades[trades["asset_class"] != "rates"].reset_index(drop=True)
        self._bars = add_indicators(aggregate_trades(trades, "1min"))
        self._alerts = detect_alerts(self._bars)

    @property
    def available(self) -> bool:
        return True

    def bars(self, symbol: str, limit: int) -> list[dict[str, Any]]:
        self._load()
        assert self._bars is not None
        frame = self._bars[self._bars["symbol"] == symbol.upper()].tail(limit)
        return df_to_records(frame)

    def alerts(self, symbol: str, limit: int) -> list[dict[str, Any]]:
        self._load()
        return [alert for alert in self._alerts if alert["symbol"] == symbol.upper()][-limit:]

    def latest(self, symbol: str) -> dict[str, Any] | None:
        self._load()
        assert self._bars is not None
        frame = self._bars[self._bars["symbol"] == symbol.upper()]
        if frame.empty:
            return None
        row = frame.iloc[-1]
        return {
            "symbol": row["symbol"],
            "price": float(row["close"]),
            "event_time": str(row["window_start"]),
            "source": "demo",
        }
