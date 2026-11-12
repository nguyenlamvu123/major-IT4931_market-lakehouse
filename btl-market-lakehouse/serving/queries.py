"""Every CQL statement market-api runs, declared as data.

Keeping the queries declarative lets ``serving/tests/test_queries.py`` prove, against
``contracts/cassandra.cql``, that each selected column exists and that each WHERE clause
restricts the full partition key plus a clustering prefix. Cassandra rejects anything
else at runtime (or needs ALLOW FILTERING, which this API never uses).
"""

from __future__ import annotations

from dataclasses import dataclass

KEYSPACE = "market"

BAR_COLUMNS = (
    "symbol",
    "day",
    "window_start",
    "window_end",
    "asset_class",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trade_count",
    "last_event_time",
    "updated_at",
)
INDICATOR_COLUMNS = (
    "symbol",
    "asset_class",
    "window_start",
    "close",
    "vwap",
    "return_1m",
    "sma_5",
    "sma_15",
    "ema_12",
    "ema_26",
    "macd",
    "macd_signal",
    "macd_hist",
    "rsi_14",
    "volatility_30m",
    "zscore_30m",
    "volume_zscore_30m",
    "updated_at",
)
ALERT_COLUMNS = (
    "day",
    "alert_time",
    "symbol",
    "alert_id",
    "asset_class",
    "alert_type",
    "severity",
    "price",
    "zscore",
    "volume_zscore",
    "message",
)
DAILY_COLUMNS = (
    "symbol",
    "day",
    "asset_class",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trade_count",
    "return_1d",
    "volatility_20d",
)
CORRELATION_COLUMNS = (
    "window_days",
    "as_of",
    "base_symbol",
    "quote_symbol",
    "correlation",
    "n_obs",
)
YIELD_CURVE_COLUMNS = (
    "curve",
    "day",
    "y_1m",
    "y_3m",
    "y_6m",
    "y_1y",
    "y_2y",
    "y_5y",
    "y_10y",
    "y_30y",
    "real_10y",
    "spread_10y_2y",
    "spread_10y_3m",
    "inverted",
)
CLUSTER_COLUMNS = (
    "model",
    "as_of",
    "symbol",
    "asset_class",
    "cluster",
    "mean_return",
    "volatility",
    "corr_us10y",
    "silhouette",
)


@dataclass(frozen=True)
class CqlQuery:
    table: str
    columns: tuple[str, ...]
    #: Equality restrictions, in primary-key order.
    where: tuple[str, ...] = ()
    limit: bool = False

    @property
    def cql(self) -> str:
        statement = f"SELECT {', '.join(self.columns)} FROM {KEYSPACE}.{self.table}"
        if self.where:
            statement += " WHERE " + " AND ".join(f"{column} = %s" for column in self.where)
        if self.limit:
            statement += " LIMIT %s"
        return statement


QUERIES: dict[str, CqlQuery] = {
    # "giá BTC hôm nay theo từng phút": one (symbol, day) partition, newest first.
    "ohlcv_1m": CqlQuery("ohlcv_1m", BAR_COLUMNS, ("symbol", "day"), limit=True),
    "ohlcv_5m": CqlQuery("ohlcv_5m", BAR_COLUMNS, ("symbol", "day"), limit=True),
    "indicators_one": CqlQuery("indicators_latest", INDICATOR_COLUMNS, ("symbol",)),
    # indicators_latest has one row per symbol (~12 rows): a full read is cheap and bounded.
    "indicators_all": CqlQuery("indicators_latest", INDICATOR_COLUMNS),
    "alerts_day": CqlQuery("alerts_by_day", ALERT_COLUMNS, ("day",), limit=True),
    "daily_symbol": CqlQuery("daily_summary", DAILY_COLUMNS, ("symbol",), limit=True),
    # as_of is the first clustering column (DESC), so LIMIT 1 returns the newest run.
    "correlation_latest_as_of": CqlQuery(
        "correlation_matrix", ("as_of",), ("window_days",), limit=True
    ),
    "correlation_rows": CqlQuery("correlation_matrix", CORRELATION_COLUMNS, ("window_days", "as_of")),
    "yield_curve": CqlQuery("yield_curve", YIELD_CURVE_COLUMNS, ("curve",), limit=True),
    "clusters_latest_as_of": CqlQuery("asset_clusters", ("as_of",), ("model",), limit=True),
    "clusters_rows": CqlQuery("asset_clusters", CLUSTER_COLUMNS, ("model", "as_of")),
}
