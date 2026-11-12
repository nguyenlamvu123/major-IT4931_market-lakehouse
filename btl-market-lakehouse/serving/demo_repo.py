"""Synthetic repository so the API and dashboards run without Cassandra.

It derives every table from the shared ``common`` pipeline (synthetic market -> bars ->
indicators -> alerts -> daily -> correlation/yield curve), so a demo dashboard shows the
same shapes the real speed and batch layers write. Rows carry exactly the contract
columns of ``serving.queries``. Responses are labelled ``X-Data-Source: demo`` by the API.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import pandas as pd

from common.alerts import detect_alerts
from common.analytics import correlation_matrix, daily_summary, yield_curve
from common.contracts import synthetic_trades, synthetic_yields
from common.indicators import add_indicators
from common.io_utils import df_to_records
from common.ohlcv import aggregate_trades

from .queries import (
    ALERT_COLUMNS,
    BAR_COLUMNS,
    CLUSTER_COLUMNS,
    CORRELATION_COLUMNS,
    DAILY_COLUMNS,
    INDICATOR_COLUMNS,
    YIELD_CURVE_COLUMNS,
)
from .repository import Row

SEED = 42
DAYS = 90
ROWS = 6_000
INTERVALS = {"1m": ("1min", timedelta(minutes=1)), "5m": ("5min", timedelta(minutes=5))}


def _project(frame: pd.DataFrame, columns: tuple[str, ...]) -> list[Row]:
    present = frame.reindex(columns=list(columns))
    return df_to_records(present)


@dataclass
class _Tables:
    bars: dict[str, pd.DataFrame]
    indicators: pd.DataFrame
    alerts: pd.DataFrame
    daily: pd.DataFrame
    correlation: pd.DataFrame
    curve: pd.DataFrame
    clusters: pd.DataFrame


def _bar_table(trades: pd.DataFrame, freq: str, width: timedelta) -> pd.DataFrame:
    bars = aggregate_trades(trades, freq)
    bars["day"] = bars["window_start"].dt.strftime("%Y-%m-%d")
    bars["window_end"] = bars["window_start"] + width
    # Synthetic trades sit on window starts; the real speed layer stores the newest trade.
    bars["last_event_time"] = bars["window_start"]
    bars["updated_at"] = None
    return bars


def _clusters(daily: pd.DataFrame, correlation: pd.DataFrame) -> pd.DataFrame:
    """Stand-in for role C's MLlib KMeans: volatility terciles (0 = calmest).

    Only meant to give the dashboard panel a realistic shape offline; ``model`` says
    "demo" so nobody mistakes it for the trained model.
    """
    stats = daily.groupby("symbol").agg(
        asset_class=("asset_class", "first"),
        mean_return=("return_1d", "mean"),
        volatility=("return_1d", "std"),
    )
    stats["cluster"] = pd.qcut(stats["volatility"].rank(method="first"), 3, labels=False)
    us10y = {}
    for row in correlation.itertuples():
        if row.quote_symbol == "US10Y":
            us10y[row.base_symbol] = row.correlation
        elif row.base_symbol == "US10Y":
            us10y[row.quote_symbol] = row.correlation
    stats["corr_us10y"] = stats.index.map(us10y)
    stats["model"] = "demo"
    stats["as_of"] = daily["day"].max()
    stats["silhouette"] = None
    return stats.reset_index()


class DemoRepository:
    source = "demo"

    def __init__(self) -> None:
        self._tables: _Tables | None = None

    def _load(self) -> _Tables:
        # Built once per process (~1-2 s); the synthetic market is deterministic.
        if self._tables is not None:
            return self._tables
        trades = pd.DataFrame(synthetic_trades(rows=ROWS, seed=SEED, days=DAYS))
        bars = {
            interval: _bar_table(trades, freq, width)
            for interval, (freq, width) in INTERVALS.items()
        }
        enriched = add_indicators(bars["1m"])
        indicators = enriched.sort_values("window_start").groupby("symbol").tail(1).copy()
        indicators["updated_at"] = None
        alerts = pd.DataFrame(detect_alerts(enriched))
        daily = daily_summary(enriched)
        rates = pd.DataFrame(synthetic_yields(days=DAYS, seed=SEED))
        correlation = correlation_matrix(daily, window_days=90, rates=rates)
        curve = yield_curve(rates)
        curve["curve"] = "UST"
        self._tables = _Tables(
            bars=bars,
            indicators=indicators,
            alerts=alerts,
            daily=daily,
            correlation=correlation,
            curve=curve,
            clusters=_clusters(daily, correlation),
        )
        return self._tables

    def default_day(self) -> str:
        # The synthetic market ends in the past, so "today" would be an empty partition.
        # Use the last US trading day (SPY traded), when every symbol has bars; the last
        # calendar day can be a weekend with crypto only.
        bars = self._load().bars["1m"]
        return str(bars.loc[bars["symbol"] == "SPY", "day"].max())

    def ohlcv(self, symbol: str, day: str, interval: str, limit: int) -> list[Row]:
        bars = self._load().bars[interval]
        rows = bars[(bars["symbol"] == symbol) & (bars["day"] == day)]
        rows = rows.sort_values("window_start", ascending=False).head(limit)
        return _project(rows, BAR_COLUMNS)

    def indicators(self, symbol: str | None) -> list[Row]:
        rows = self._load().indicators
        if symbol:
            rows = rows[rows["symbol"] == symbol]
        return _project(rows.sort_values("symbol"), INDICATOR_COLUMNS)

    def alerts(self, day: str, limit: int) -> list[Row]:
        alerts = self._load().alerts
        if alerts.empty:
            return []
        rows = alerts[alerts["day"] == day].sort_values("alert_time", ascending=False)
        return _project(rows.head(limit), ALERT_COLUMNS)

    def daily(self, symbol: str, limit: int) -> list[Row]:
        daily = self._load().daily
        rows = daily[daily["symbol"] == symbol].sort_values("day", ascending=False)
        return _project(rows.head(limit), DAILY_COLUMNS)

    def correlation(self, window_days: int) -> list[Row]:
        rows = self._load().correlation
        rows = rows[rows["window_days"] == window_days]
        return _project(rows, CORRELATION_COLUMNS)

    def yield_curve(self, limit: int) -> list[Row]:
        rows = self._load().curve.sort_values("day", ascending=False).head(limit)
        return _project(rows, YIELD_CURVE_COLUMNS)

    def clusters(self) -> list[Row]:
        return _project(self._load().clusters.sort_values("symbol"), CLUSTER_COLUMNS)
