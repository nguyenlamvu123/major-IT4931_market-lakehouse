"""Indicator and alert state kept across micro-batches (pure pandas, no Spark).

Why state is needed: in update mode a micro-batch carries only the bars that received
trades since the previous trigger, usually one bar per symbol. Indicators such as the
30-bar z-score, RSI-14 or MACD(12, 26, 9) need tens of previous bars, so computing them
on the batch alone (the v1 behaviour) yields NaN and alerts never fire.

State model (all of it lives in the driver process, ``foreachBatch`` runs there):

- ``_bars[symbol]``: runtime buffer of the newest ``lookback`` one-minute bars, keyed by
  ``window_start``. A partial bar is replaced when the same window arrives again.
- Hydration: the first time a symbol is seen by this driver (fresh start or restart
  after a crash), ``loader(symbol, as_of)`` restores recent bars from Cassandra
  ``ohlcv_1m``, so a restart does not wipe 30 minutes of context.
- ``_emitted``: bounded FIFO of alert ids already published to Kafka. Cassandra writes
  are upserts and can safely repeat; Kafka writes cannot, so a partial bar that keeps
  spiking over several triggers is published once. The set is lost on driver restart,
  hence Kafka alerts are at-least-once and consumers dedupe by ``alert_id``.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pandas as pd

from common.alerts import detect_alerts
from common.indicators import add_indicators

from .columns import ALERT_COLUMNS, INDICATOR_COLUMNS

#: ``(symbol, as_of) -> recent 1m bars`` restored from storage (Cassandra ohlcv_1m).
BarLoader = Callable[[str, pd.Timestamp], pd.DataFrame]

BAR_FIELDS = [
    "symbol",
    "asset_class",
    "window_start",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trade_count",
]


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


@dataclass
class StateUpdate:
    #: ``indicators_latest`` rows for every symbol touched by the batch.
    indicators: pd.DataFrame
    #: Alerts on the bars of this batch, for the Cassandra upsert (may repeat).
    alerts: pd.DataFrame
    #: Subset of ``alerts`` never published to Kafka by this driver.
    new_alerts: pd.DataFrame


def _normalise(bars: pd.DataFrame) -> pd.DataFrame:
    frame = bars.copy()
    for column in BAR_FIELDS:
        if column not in frame.columns:
            frame[column] = None
    frame = frame[BAR_FIELDS]
    frame["window_start"] = pd.to_datetime(frame["window_start"], utc=True)
    return frame


class IndicatorState:
    """Rolling per-symbol bar buffer that turns micro-batches into indicators/alerts."""

    def __init__(
        self,
        lookback: int = 60,
        loader: BarLoader | None = None,
        max_emitted: int = 10_000,
        z_threshold: float = 3.0,
        volume_z_threshold: float = 3.0,
    ) -> None:
        # 60 one-minute bars cover the 30-bar z-score baseline and the MACD(12, 26, 9)
        # warm-up (35 bars) with margin.
        self.lookback = lookback
        self.loader = loader
        self.z_threshold = z_threshold
        self.volume_z_threshold = volume_z_threshold
        self._bars: dict[str, pd.DataFrame] = {}
        self._emitted_order: deque[str] = deque()
        self._emitted: set[str] = set()
        self._max_emitted = max_emitted

    def buffer(self, symbol: str) -> pd.DataFrame:
        return self._bars.get(symbol, pd.DataFrame(columns=BAR_FIELDS)).copy()

    def _hydrate(self, symbol: str, as_of: pd.Timestamp) -> pd.DataFrame:
        if self.loader is None:
            return pd.DataFrame(columns=BAR_FIELDS)
        restored = self.loader(symbol, as_of)
        if restored is None or restored.empty:
            return pd.DataFrame(columns=BAR_FIELDS)
        return _normalise(restored)

    def _merge(self, symbol: str, incoming: pd.DataFrame) -> pd.DataFrame:
        if symbol in self._bars:
            current = self._bars[symbol]
        else:
            current = self._hydrate(symbol, incoming["window_start"].max())
        parts = [frame for frame in (current, incoming) if not frame.empty]
        merged = (
            pd.concat(parts, ignore_index=True)
            # A bar seen again (partial -> fuller, or replayed batch) replaces the old one.
            .drop_duplicates(subset=["window_start"], keep="last")
            .sort_values("window_start")
            .tail(self.lookback)
            .reset_index(drop=True)
        )
        self._bars[symbol] = merged
        return merged

    def _mark_emitted(self, alert_id: str) -> bool:
        """Remember ``alert_id``; return False when it was already published."""
        if alert_id in self._emitted:
            return False
        self._emitted.add(alert_id)
        self._emitted_order.append(alert_id)
        if len(self._emitted_order) > self._max_emitted:
            self._emitted.discard(self._emitted_order.popleft())
        return True

    def update(self, bars: pd.DataFrame, updated_at: pd.Timestamp) -> StateUpdate:
        """Fold one micro-batch of 1m bars into the state; derive indicators and alerts."""
        if bars is None or bars.empty:
            empty_alerts = pd.DataFrame(columns=ALERT_COLUMNS)
            return StateUpdate(pd.DataFrame(columns=INDICATOR_COLUMNS), empty_alerts, empty_alerts)

        incoming = _normalise(bars)
        enriched_parts: list[pd.DataFrame] = []
        changed_parts: list[pd.DataFrame] = []
        for symbol, group in incoming.groupby("symbol", sort=True):
            buffer = self._merge(str(symbol), group)
            enriched = add_indicators(buffer)
            enriched_parts.append(enriched)
            # Alerts are only evaluated on bars this batch touched; older bars were
            # evaluated when they changed.
            changed_parts.append(enriched[enriched["window_start"].isin(group["window_start"])])

        enriched_all = pd.concat(enriched_parts, ignore_index=True)
        changed = pd.concat(changed_parts, ignore_index=True)
        alerts = alert_rows(
            detect_alerts(
                changed,
                z_threshold=self.z_threshold,
                volume_z_threshold=self.volume_z_threshold,
            )
        )
        fresh_mask = [self._mark_emitted(alert_id) for alert_id in alerts["alert_id"]]
        new_alerts = alerts[pd.Series(fresh_mask, index=alerts.index, dtype=bool)]
        return StateUpdate(
            indicators=indicator_rows(enriched_all, updated_at),
            alerts=alerts,
            new_alerts=new_alerts.reset_index(drop=True),
        )
