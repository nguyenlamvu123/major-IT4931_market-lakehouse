"""Column names shared by Spark schemas and pure batch logic (no pyspark import)."""

BAR_COLUMNS = [
    "symbol",
    "window_start",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trade_count",
    "sma_5",
    "sma_15",
    "ema_12",
    "ema_26",
    "macd",
    "macd_signal",
    "macd_hist",
    "rsi_14",
    "return_1m",
    "volatility_30m",
    "zscore_30m",
    "volume_zscore_30m",
]

ALERT_COLUMNS = [
    "alert_id",
    "symbol",
    "alert_time",
    "alert_type",
    "severity",
    "price",
    "zscore",
    "volume_zscore",
    "message",
]
