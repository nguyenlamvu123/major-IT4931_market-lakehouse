"""Daily summary, cross-asset correlation, yield curve and ML features (pandas reference).

These are the reference implementations for the batch layer. The Spark jobs on the C
branch must produce the same numbers on the same input; their tests compare against
these functions.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .contracts import YIELD_SYMBOLS

#: Contract tenor -> column of the ``yield_curve`` gold table / Cassandra table.
TENOR_COLUMNS = {
    "1M": "y_1m",
    "3M": "y_3m",
    "6M": "y_6m",
    "1Y": "y_1y",
    "2Y": "y_2y",
    "5Y": "y_5y",
    "10Y": "y_10y",
    "30Y": "y_30y",
    "REAL10Y": "real_10y",
}
YIELD_CURVE_COLUMNS = [
    "day",
    *TENOR_COLUMNS.values(),
    "spread_10y_2y",
    "spread_10y_3m",
    "inverted",
]
CORRELATION_COLUMNS = [
    "as_of",
    "window_days",
    "base_symbol",
    "quote_symbol",
    "correlation",
    "n_obs",
]


def daily_summary(bars: pd.DataFrame) -> pd.DataFrame:
    """Collapse intraday bars into one row per symbol and UTC day.

    ``return_1d`` is close-to-close on the symbol's own trading days and
    ``volatility_20d`` is the rolling std of those returns (not annualised).
    """
    required = {"symbol", "window_start", "open", "high", "low", "close", "volume", "vwap"}
    missing = required.difference(bars.columns)
    if missing:
        raise ValueError(f"daily_summary missing columns: {sorted(missing)}")
    columns = [
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
    ]
    if bars.empty:
        return pd.DataFrame(columns=columns)

    frame = bars.sort_values(["symbol", "window_start"]).copy()
    frame["day"] = pd.to_datetime(frame["window_start"], utc=True).dt.strftime("%Y-%m-%d")
    frame["notional"] = frame["vwap"].fillna(frame["close"]) * frame["volume"]
    if "trade_count" not in frame.columns:
        frame["trade_count"] = 1
    if "asset_class" not in frame.columns:
        frame["asset_class"] = None
    daily = frame.groupby(["symbol", "day"], as_index=False).agg(
        asset_class=("asset_class", "first"),
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        notional=("notional", "sum"),
        trade_count=("trade_count", "sum"),
    )
    # Daily VWAP is volume-weighted across the day's bars; undefined without volume.
    daily["vwap"] = daily["notional"] / daily["volume"].replace(0, np.nan)
    daily = daily.drop(columns=["notional"]).sort_values(["symbol", "day"])
    daily["return_1d"] = daily.groupby("symbol")["close"].pct_change(fill_method=None)
    daily["volatility_20d"] = daily.groupby("symbol")["return_1d"].transform(
        lambda returns: returns.rolling(20, min_periods=5).std()
    )
    return daily[columns].reset_index(drop=True)


def yield_levels(rates: pd.DataFrame) -> pd.DataFrame:
    """Long ``rates.yields`` records -> wide levels per day (one column per tenor)."""
    required = {"day", "tenor", "yield"}
    missing = required.difference(rates.columns)
    if missing:
        raise ValueError(f"yield_levels missing columns: {sorted(missing)}")
    if rates.empty:
        return pd.DataFrame(columns=["day"])
    return (
        rates.pivot_table(index="day", columns="tenor", values="yield", aggfunc="last")
        .sort_index()
        .reset_index()
        .rename_axis(columns=None)
    )


def aligned_daily_panel(
    daily: pd.DataFrame,
    rates: pd.DataFrame | None = None,
    reference_symbol: str = "SPY",
    ffill_limit: int = 3,
) -> pd.DataFrame:
    """Put every series on one trading calendar, ready for return/correlation math.

    Calendar policy (slide "Thách thức dữ liệu" of the idea PDF):

    - The reference calendar is the days ``reference_symbol`` traded (US stock days).
      If that symbol is absent, the union of all observed days is used instead.
    - Crypto trades 24/7: its weekend rows are dropped by the left join, so its returns
      are measured between the same stock-market days as everything else.
    - The bond market closes on some days the stock market is open (Columbus Day,
      Veterans Day): yields are forward-filled for at most ``ffill_limit`` days, so a
      stale level is never carried across a long data gap.

    Returns a wide frame indexed by ``day``: price symbols hold closes, yield
    pseudo-symbols (``US2Y``, ``US10Y``, ``REAL10Y``) hold yield levels in percent.
    """
    closes = daily.pivot_table(index="day", columns="symbol", values="close", aggfunc="last")
    if rates is not None and not rates.empty:
        levels = yield_levels(rates).set_index("day")
        levels = levels[[tenor for tenor in YIELD_SYMBOLS if tenor in levels.columns]]
        levels = levels.rename(columns=YIELD_SYMBOLS)
        closes = closes.join(levels, how="outer")
    closes = closes.sort_index()

    if reference_symbol in closes.columns:
        calendar = closes.index[closes[reference_symbol].notna()]
    else:
        calendar = closes.index
    return closes.ffill(limit=ffill_limit).loc[calendar].rename_axis(columns=None)


def panel_changes(panel: pd.DataFrame) -> pd.DataFrame:
    """Prices -> simple returns; yields -> changes in percentage points.

    Correlating a yield *level* with a price *return* would be meaningless, so yields
    enter the matrix as daily changes.
    """
    yield_columns = [column for column in panel.columns if column in YIELD_SYMBOLS.values()]
    price_columns = [column for column in panel.columns if column not in yield_columns]
    changes = panel[price_columns].pct_change(fill_method=None)
    for column in yield_columns:
        changes[column] = panel[column].diff()
    return changes


def correlation_matrix(
    daily: pd.DataFrame,
    window_days: int = 90,
    rates: pd.DataFrame | None = None,
    reference_symbol: str = "SPY",
    min_obs: int = 3,
) -> pd.DataFrame:
    """Pairwise correlation of daily changes over the last ``window_days`` calendar rows.

    Each unordered pair appears once (``base_symbol < quote_symbol``). ``as_of`` is the
    last day of the window; ``n_obs`` counts the days where both series changed.
    """
    required = {"symbol", "day", "close"}
    missing = required.difference(daily.columns)
    if missing:
        raise ValueError(f"correlation_matrix missing columns: {sorted(missing)}")
    if daily.empty:
        return pd.DataFrame(columns=CORRELATION_COLUMNS)

    panel = aligned_daily_panel(daily, rates, reference_symbol=reference_symbol)
    changes = panel_changes(panel).iloc[1:].tail(window_days)
    if len(changes) < 2 or changes.shape[1] < 2:
        return pd.DataFrame(columns=CORRELATION_COLUMNS)

    as_of = str(changes.index.max())
    corr = changes.corr(min_periods=min_obs)
    present = changes.notna().astype(int)
    overlap = present.T.dot(present)
    records: list[dict[str, object]] = []
    for base in corr.index:
        for quote in corr.columns:
            if str(base) >= str(quote):
                continue
            value = corr.loc[base, quote]
            if pd.isna(value):
                continue
            records.append(
                {
                    "as_of": as_of,
                    "window_days": int(window_days),
                    "base_symbol": str(base),
                    "quote_symbol": str(quote),
                    "correlation": round(float(value), 6),
                    "n_obs": int(overlap.loc[base, quote]),
                }
            )
    return pd.DataFrame.from_records(records, columns=CORRELATION_COLUMNS)


def yield_curve(rates: pd.DataFrame) -> pd.DataFrame:
    """Wide daily Treasury curve with the spreads used by the inversion question.

    ``inverted`` is true when the 10Y-2Y spread is negative (the classic recession
    signal discussed in the idea PDF).
    """
    if rates.empty:
        return pd.DataFrame(columns=YIELD_CURVE_COLUMNS)
    curve = yield_levels(rates).rename(columns=TENOR_COLUMNS)
    for column in TENOR_COLUMNS.values():
        if column not in curve.columns:
            curve[column] = np.nan
    curve["spread_10y_2y"] = curve["y_10y"] - curve["y_2y"]
    curve["spread_10y_3m"] = curve["y_10y"] - curve["y_3m"]
    curve["inverted"] = curve["spread_10y_2y"] < 0
    return curve[YIELD_CURVE_COLUMNS].reset_index(drop=True)


def build_ml_features(daily: pd.DataFrame, windows: tuple[int, ...] = (5, 20)) -> pd.DataFrame:
    """Build a simple market-wide return + rolling-volatility feature table per day."""
    required = {"symbol", "day", "close"}
    missing = required.difference(daily.columns)
    if missing:
        raise ValueError(f"build_ml_features missing columns: {sorted(missing)}")
    if daily.empty:
        return pd.DataFrame()

    pivot = daily.pivot_table(index="day", columns="symbol", values="close").sort_index()
    returns = pivot.pct_change(fill_method=None)
    output = pd.DataFrame(index=returns.index)
    for window in windows:
        output[f"ret_mean_{window}d"] = returns.rolling(window, min_periods=2).mean().mean(axis=1)
        output[f"ret_std_{window}d"] = returns.rolling(window, min_periods=2).std().mean(axis=1)
    output["breadth"] = (returns > 0).sum(axis=1) / returns.notna().sum(axis=1).replace(0, np.nan)
    return output.dropna(how="all").reset_index()
