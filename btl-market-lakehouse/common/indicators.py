"""Technical indicators shared by the streaming layer and the local demo."""

from __future__ import annotations

import numpy as np
import pandas as pd


def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window, min_periods=1).mean()


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def rolling_zscore(series: pd.Series, window: int = 30, min_periods: int = 5) -> pd.Series:
    """Distance of each value from the *previous* ``window`` values, in std units.

    The baseline is shifted by one row so a spike does not inflate its own mean/std.
    This matches the alert rule in the PDF: "giá lệch hơn 3 độ lệch chuẩn so với
    30 phút trước".
    """
    baseline = series.shift(1)
    mean = baseline.rolling(window, min_periods=min_periods).mean()
    std = baseline.rolling(window, min_periods=min_periods).std()
    return (series - mean) / std.replace(0, np.nan)


def rsi(series: pd.Series, window: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
    avg_loss = loss.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    result = 100 - (100 / (1 + rs))
    result = result.mask((avg_loss == 0) & (avg_gain > 0), 100.0)
    result = result.mask((avg_loss == 0) & (avg_gain == 0), 50.0)
    return result


def macd(
    series: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    line = ema(series, fast) - ema(series, slow)
    signal_line = ema(line, signal)
    return line, signal_line, line - signal_line


def add_indicators(bars: pd.DataFrame) -> pd.DataFrame:
    """Add MA, RSI, MACD, returns, volatility and z-score columns per symbol."""
    required = {"symbol", "window_start", "close", "volume"}
    missing = required.difference(bars.columns)
    if missing:
        raise ValueError(f"add_indicators missing columns: {sorted(missing)}")
    if bars.empty:
        return bars.copy()

    parts: list[pd.DataFrame] = []
    for _, group in bars.groupby("symbol", sort=False):
        frame = group.sort_values("window_start").copy()
        frame["sma_5"] = sma(frame["close"], 5)
        frame["sma_15"] = sma(frame["close"], 15)
        frame["ema_12"] = ema(frame["close"], 12)
        frame["ema_26"] = ema(frame["close"], 26)
        line, signal_line, hist = macd(frame["close"])
        frame["macd"] = line
        frame["macd_signal"] = signal_line
        frame["macd_hist"] = hist
        frame["rsi_14"] = rsi(frame["close"], 14)
        frame["return_1m"] = frame["close"].pct_change(fill_method=None)
        frame["volatility_30m"] = (
            frame["return_1m"].rolling(30, min_periods=5).std()
        )
        frame["zscore_30m"] = rolling_zscore(frame["close"], 30)
        frame["volume_zscore_30m"] = rolling_zscore(frame["volume"], 30)
        parts.append(frame)

    return (
        pd.concat(parts, ignore_index=True)
        .sort_values(["symbol", "window_start"])
        .reset_index(drop=True)
    )
