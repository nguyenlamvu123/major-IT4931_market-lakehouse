"""Silver -> gold analytics in Spark: daily summary, cross-asset correlation, yield curve.

Each function mirrors a pandas reference in ``common.analytics``; the tests run both on
the same synthetic market and require identical numbers (within 1e-6). The pandas
versions document the maths, this module is what runs on the cluster.

Gold partitions (``dt``) are the *processing* day. ``daily_summary`` and ``yield_curve``
rows of day D land in ``dt=D``. ``correlation_matrix`` and ``ml_features`` for
processing day D are computed "as of D", i.e. over the history up to D.
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common.analytics import TENOR_COLUMNS
from common.contracts import YIELD_SYMBOLS

from .lake import Lake, existing_paths
from .silver import read_silver

DAILY_COLUMNS = [
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
CORRELATION_COLUMNS = [
    "as_of",
    "window_days",
    "base_symbol",
    "quote_symbol",
    "correlation",
    "n_obs",
]
YIELD_CURVE_COLUMNS = [
    "curve",
    "day",
    *TENOR_COLUMNS.values(),
    "spread_10y_2y",
    "spread_10y_3m",
    "inverted",
]
ML_FEATURE_COLUMNS = [
    "as_of",
    "symbol",
    "asset_class",
    "mean_return",
    "volatility",
    "avg_volume",
    "corr_us10y",
    "n_days",
]

_YIELD_DDL = ", ".join(f"{column} double" for column in TENOR_COLUMNS.values())

#: Gold schemas as DDL. Readers (Cassandra sink, KMeans) pass them explicitly so a table
#: whose processed days had no rows (no yields on a weekend) still reads as empty.
GOLD_DDL = {
    "daily_summary": (
        "symbol string, day date, asset_class string, open double, high double, low double, "
        "close double, volume double, vwap double, trade_count long, return_1d double, "
        "volatility_20d double, price_source string, dt date"
    ),
    "correlation_matrix": (
        "dt date, as_of date, window_days int, base_symbol string, quote_symbol string, "
        "correlation double, n_obs int"
    ),
    "yield_curve": (
        f"curve string, day date, {_YIELD_DDL}, spread_10y_2y double, "
        "spread_10y_3m double, inverted boolean, dt date"
    ),
    "ml_features": (
        "dt date, as_of date, symbol string, asset_class string, mean_return double, "
        "volatility double, avg_volume double, corr_us10y double, n_days int"
    ),
    "asset_clusters": (
        "model string, as_of date, symbol string, asset_class string, cluster int, "
        "mean_return double, volatility double, corr_us10y double, silhouette double, dt date"
    ),
}

#: Daily price sources in precedence order (1 wins). See combine_daily_sources.
PRICE_SOURCES = {"bar_1d": 1, "trades": 2, "bar_1m": 3}


def _vwap() -> Column:
    # NULL (not NaN, not an error) when the day has no volume.
    return F.when(F.col("volume") != 0, F.col("notional") / F.col("volume"))


def daily_from_trades(trades: DataFrame) -> DataFrame:
    """One OHLCV row per (symbol, UTC day) from silver trades.

    VWAP is sum(price*volume)/sum(volume) and stays NULL for days without volume (the
    dollar index has none on Yahoo).
    """
    return (
        trades.withColumn("day", F.to_date("event_time"))
        .groupBy("symbol", "day")
        .agg(
            F.first("asset_class").alias("asset_class"),
            F.min_by("price", "event_time_ms").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "event_time_ms").alias("close"),
            F.sum("volume").alias("volume"),
            F.sum(F.col("price") * F.col("volume")).alias("notional"),
            F.count(F.lit(1)).alias("trade_count"),
        )
        .withColumn("vwap", _vwap())
        .drop("notional")
        .withColumn("price_source", F.lit("trades"))
    )


def daily_from_bars(bars: DataFrame) -> DataFrame:
    """Daily rows from silver bars: 1Day bars as-is, 1Min bars aggregated per day."""
    day_bars = (
        bars.filter(F.col("timeframe") == "1Day")
        .withColumn("day", F.to_date("event_time"))
        .withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy("symbol", "day").orderBy(F.col("ingest_time").desc_nulls_last())
            ),
        )
        .filter(F.col("_rank") == 1)
        .select(
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
            F.lit("bar_1d").alias("price_source"),
        )
    )
    minute_bars = (
        bars.filter(F.col("timeframe") == "1Min")
        .withColumn("day", F.to_date("event_time"))
        .groupBy("symbol", "day")
        .agg(
            F.first("asset_class").alias("asset_class"),
            F.min_by("open", "event_time_ms").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.max_by("close", "event_time_ms").alias("close"),
            F.sum("volume").alias("volume"),
            F.sum(F.coalesce("vwap", "close") * F.col("volume")).alias("notional"),
            F.sum("trade_count").alias("trade_count"),
        )
        .withColumn("vwap", _vwap())
        .drop("notional")
        .withColumn("price_source", F.lit("bar_1m"))
    )
    return day_bars.unionByName(minute_bars)


def combine_daily_sources(*sources: DataFrame) -> DataFrame:
    """Pick one daily row per (symbol, day) by source precedence.

    1. ``bar_1d``: yfinance daily bars (backfill). They come from the consolidated tape,
       so volume covers every US venue; the live IEX feed only sees a few percent of it.
    2. ``trades``: tick-level live trades. Used for every day the backfill does not
       cover yet (today, and all crypto/gold/DXY days after the backfill date).
    3. ``bar_1m``: Alpaca minute bars. Only used when the trade stream had a gap but the
       bar stream did not, so the day is still present for returns and correlations.
    """
    combined = sources[0]
    for frame in sources[1:]:
        combined = combined.unionByName(frame)
    priority = F.create_map(
        *[item for pair in PRICE_SOURCES.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )
    window = Window.partitionBy("symbol", "day").orderBy(priority[F.col("price_source")])
    return (
        combined.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def add_returns(daily: DataFrame) -> DataFrame:
    """Close-to-close return on the symbol's own trading days + 20-row rolling std.

    Matches pandas ``pct_change`` and ``rolling(20, min_periods=5).std()``: the window
    covers 20 rows, NULL returns are ignored, and fewer than 5 returns give NULL.
    """
    by_symbol = Window.partitionBy("symbol").orderBy("day")
    rolling = by_symbol.rowsBetween(-19, Window.currentRow)
    with_return = daily.withColumn(
        "return_1d", F.col("close") / F.lag("close").over(by_symbol) - 1
    )
    return with_return.withColumn(
        "volatility_20d",
        F.when(
            F.count("return_1d").over(rolling) >= 5,
            F.stddev_samp("return_1d").over(rolling),
        ),
    )


def build_daily(trades: DataFrame, bars: DataFrame) -> DataFrame:
    return add_returns(
        combine_daily_sources(daily_from_trades(trades), daily_from_bars(bars))
    ).select(*DAILY_COLUMNS, "price_source")


def _yield_series(yields: DataFrame) -> DataFrame:
    mapping = F.create_map(
        *[item for pair in YIELD_SYMBOLS.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )
    return (
        yields.filter(F.col("tenor").isin(list(YIELD_SYMBOLS)))
        .select("day", mapping[F.col("tenor")].alias("series"), F.col("yield").alias("value"))
        .withColumn("is_yield", F.lit(True))
    )


def aligned_changes(
    daily: DataFrame,
    yields: DataFrame,
    reference_symbol: str = "SPY",
    ffill_limit: int = 3,
) -> DataFrame:
    """Long table (day, series, change) on the reference trading calendar.

    Same policy as ``common.analytics.aligned_daily_panel``:

    - every series gets a row for every day any series was observed (prices + yields),
    - gaps are forward-filled for at most ``ffill_limit`` rows (a bond-only holiday
      carries the previous yield; a long outage does not carry a stale level),
    - only days where ``reference_symbol`` itself traded are kept, which drops crypto's
      weekend rows and aligns every series on US stock days,
    - prices become simple returns, yields become changes in percentage points.
    """
    prices = daily.select(
        "day", F.col("symbol").alias("series"), F.col("close").alias("value")
    ).withColumn("is_yield", F.lit(False))
    observed = prices.unionByName(_yield_series(yields))

    days = observed.select("day").distinct()
    series = observed.select("series", "is_yield").distinct()
    grid = days.crossJoin(series).join(
        observed.select("day", "series", "value"), ["day", "series"], "left"
    )
    fill_window = (
        Window.partitionBy("series").orderBy("day").rowsBetween(-ffill_limit, Window.currentRow)
    )
    filled = grid.withColumn("filled", F.last("value", ignorenulls=True).over(fill_window))

    reference_days = observed.filter(F.col("series") == reference_symbol).select("day")
    # Without the reference symbol (e.g. a crypto-only test run) every observed day is a
    # calendar day, like the pandas reference.
    calendar = (
        reference_days.distinct()
        if reference_days.limit(1).count() > 0
        else days
    )
    on_calendar = filled.join(calendar, "day", "inner")

    previous = F.lag("filled").over(Window.partitionBy("series").orderBy("day"))
    change = F.when(
        F.col("is_yield"), F.col("filled") - previous
    ).otherwise(F.col("filled") / previous - 1)
    return on_calendar.withColumn("change", change).select("day", "series", "change")


def correlation_as_of(
    changes: DataFrame,
    as_of_days: list[date],
    window_days: int = 90,
    min_obs: int = 3,
) -> DataFrame:
    """Pairwise Pearson correlation of daily changes, one matrix per requested day.

    For request day D the window is the last ``window_days`` calendar rows on or before
    D, and ``as_of`` is the last of those rows (a Saturday request reports Friday).
    Pairs appear once with ``base_symbol < quote_symbol``; pairs with fewer than
    ``min_obs`` common observations, or a constant series, are dropped.
    """
    spark = changes.sparkSession
    requests = spark.createDataFrame([(day,) for day in as_of_days], "request_day date")
    calendar = changes.select("day").distinct()
    ranked = (
        requests.join(calendar, F.col("day") <= F.col("request_day"))
        .withColumn(
            "_rank",
            F.row_number().over(Window.partitionBy("request_day").orderBy(F.col("day").desc())),
        )
        .filter(F.col("_rank") <= window_days)
        .withColumn("as_of", F.max("day").over(Window.partitionBy("request_day")))
        .drop("_rank")
    )
    windowed = ranked.join(changes.filter(F.col("change").isNotNull()), "day")
    left = windowed.select(
        "request_day", "as_of", "day", F.col("series").alias("base_symbol"),
        F.col("change").alias("x"),
    )
    right = windowed.select(
        "request_day", "day", F.col("series").alias("quote_symbol"), F.col("change").alias("y")
    )
    pairs = left.join(right, ["request_day", "day"]).filter(
        F.col("base_symbol") < F.col("quote_symbol")
    )
    result = (
        pairs.groupBy("request_day", "as_of", "base_symbol", "quote_symbol")
        .agg(F.corr("x", "y").alias("correlation"), F.count(F.lit(1)).alias("n_obs"))
        .filter(
            (F.col("n_obs") >= min_obs)
            & F.col("correlation").isNotNull()
            & ~F.isnan("correlation")
        )
    )
    return result.select(
        F.col("request_day").alias("dt"),
        "as_of",
        F.lit(window_days).cast("int").alias("window_days"),
        "base_symbol",
        "quote_symbol",
        F.round("correlation", 6).alias("correlation"),
        F.col("n_obs").cast("int").alias("n_obs"),
    )


def build_yield_curve(yields: DataFrame) -> DataFrame:
    """Wide daily Treasury curve; ``inverted`` = 10Y-2Y spread below zero (false if unknown)."""
    tenors = list(TENOR_COLUMNS)
    wide = yields.groupBy("day").pivot("tenor", tenors).agg(F.last("yield"))
    for tenor, column in TENOR_COLUMNS.items():
        wide = wide.withColumnRenamed(tenor, column)
    spread = F.col("y_10y") - F.col("y_2y")
    return (
        wide.withColumn("curve", F.lit("UST"))
        .withColumn("spread_10y_2y", spread)
        .withColumn("spread_10y_3m", F.col("y_10y") - F.col("y_3m"))
        .withColumn("inverted", F.coalesce(spread < 0, F.lit(False)))
        .select(YIELD_CURVE_COLUMNS)
    )


def build_ml_features(
    daily: DataFrame, correlations: DataFrame, as_of_days: list[date], window_days: int = 90
) -> DataFrame:
    """Per-symbol features for the clustering model, as of each requested day.

    ``mean_return`` / ``volatility`` use the symbol's last ``window_days`` own trading
    days; ``corr_us10y`` is the symbol's correlation with 10Y yield changes from the
    correlation matrix of the same day (NULL if the pair had too few observations).
    """
    spark = daily.sparkSession
    requests = spark.createDataFrame([(day,) for day in as_of_days], "dt date")
    recent = (
        requests.join(daily, F.col("day") <= F.col("dt"))
        .withColumn(
            "_rank",
            F.row_number().over(Window.partitionBy("dt", "symbol").orderBy(F.col("day").desc())),
        )
        .filter(F.col("_rank") <= window_days)
    )
    features = recent.groupBy("dt", "symbol").agg(
        F.max("day").alias("as_of"),
        F.first("asset_class", ignorenulls=True).alias("asset_class"),
        F.avg("return_1d").alias("mean_return"),
        F.stddev_samp("return_1d").alias("volatility"),
        F.avg("volume").alias("avg_volume"),
        F.count("return_1d").cast("int").alias("n_days"),
    )
    us10y = correlations.filter(
        (F.col("base_symbol") == "US10Y") | (F.col("quote_symbol") == "US10Y")
    ).select(
        "dt",
        F.when(F.col("base_symbol") == "US10Y", F.col("quote_symbol"))
        .otherwise(F.col("base_symbol"))
        .alias("symbol"),
        F.col("correlation").alias("corr_us10y"),
    )
    return features.join(us10y, ["dt", "symbol"], "left").select("dt", *ML_FEATURE_COLUMNS)


def _in_days(column: str, first: date, last: date) -> Column:
    return F.col(column).between(F.lit(first), F.lit(last))


def build_gold(
    spark: SparkSession,
    lake: Lake,
    days: list[date],
    *,
    lookback_days: int = 150,
    window_days: int = 90,
    reference_symbol: str = "SPY",
) -> dict[str, DataFrame]:
    """Compute all gold tables for the processing days ``days``.

    Silver is read from ``days[0] - lookback_days`` so returns, 20-day volatility and
    the 90-row correlation window have history (90 US trading days ~ 130 calendar days).
    """
    first, last = days[0], days[-1]
    history_start = first - timedelta(days=lookback_days)
    trades = read_silver(spark, lake, "trades", history_start, last)
    bars = read_silver(spark, lake, "bars", history_start, last)
    yields = read_silver(spark, lake, "yields", history_start, last)

    daily = build_daily(trades, bars).cache()
    changes = aligned_changes(daily, yields, reference_symbol=reference_symbol).cache()
    correlations = correlation_as_of(changes, days, window_days=window_days).cache()

    return {
        "daily_summary": daily.filter(F.col("day").isin(days)).withColumn("dt", F.col("day")),
        "correlation_matrix": correlations,
        "yield_curve": build_yield_curve(yields)
        .filter(F.col("day").isin(days))
        .withColumn("dt", F.col("day")),
        "ml_features": build_ml_features(daily, correlations, days, window_days=window_days),
    }


def write_gold(lake: Lake, tables: dict[str, DataFrame]) -> None:
    """Overwrite the processed ``dt`` partitions of each gold table (dynamic overwrite)."""
    for name, frame in tables.items():
        frame.write.mode("overwrite").partitionBy("dt").parquet(lake.gold(name))


def read_gold(spark: SparkSession, lake: Lake, table: str, first: date, last: date) -> DataFrame:
    """Read gold ``table`` for processing days ``first <= dt <= last`` (empty if absent)."""
    if not existing_paths(spark, [lake.gold(table)]):
        return spark.createDataFrame([], GOLD_DDL[table])
    frame = spark.read.schema(GOLD_DDL[table]).parquet(lake.gold(table))
    return frame.filter(_in_days("dt", first, last))
