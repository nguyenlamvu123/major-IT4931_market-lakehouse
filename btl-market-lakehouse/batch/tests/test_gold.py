"""Silver -> gold in Spark must reproduce the pandas reference in common.analytics."""

from __future__ import annotations

import math
from datetime import date, timedelta

import pandas as pd
import pytest

from common.analytics import correlation_matrix, daily_summary, yield_curve
from common.contracts import INSTRUMENTS, SYNTHETIC_START
from common.ohlcv import aggregate_trades

DAYS = 90  # must match conftest.SYNTHETIC_DAYS
FIRST_DAY = SYNTHETIC_START.date()
LAST_DAY = FIRST_DAY + timedelta(days=DAYS - 1)


def _to_pandas(frame) -> pd.DataFrame:
    # NOTICE:
    # DataFrame in PySpark 3.5 imports distutils, which Python 3.12 removed
    # (pyspark/sql/pandas/utils.py, require_minimum_pandas_version). Collecting rows
    # keeps the tests independent of setuptools being installed.
    # Removal condition: PySpark >= 4.0 or a test environment that ships setuptools.
    return pd.DataFrame([row.asDict() for row in frame.collect()])


def _close(left, right) -> bool:
    if left is None or right is None or (isinstance(right, float) and math.isnan(right)):
        return (left is None or (isinstance(left, float) and math.isnan(left))) and (
            right is None or (isinstance(right, float) and math.isnan(right))
        )
    return math.isclose(float(left), float(right), rel_tol=1e-9, abs_tol=1e-6)


@pytest.fixture(scope="module")
def reference(synthetic_market):
    trades, yields = synthetic_market
    daily = daily_summary(aggregate_trades(pd.DataFrame(trades)))
    rates = pd.DataFrame(yields)
    return {
        "daily": daily,
        "correlation": correlation_matrix(daily, window_days=90, rates=rates),
        "curve": yield_curve(rates),
    }


def test_v1_gold_job_grouped_trades_by_bar_columns(spark, silver_lake) -> None:
    # ROOT CAUSE:
    #
    # The v1 silver_to_gold.py read silver *trades* (price, volume, event_time) but
    # aggregated them as if they were 1-minute bars:
    #
    #   silver.groupBy("symbol", F.to_date("window_start")).agg(min_by(open, ...), ...)
    #   -> AnalysisException: UNRESOLVED_COLUMN window_start
    #
    # We fixed this by aggregating trades directly (batch.gold.daily_from_trades) and
    # combining them with stocks.bars by an explicit source precedence.
    from pyspark.errors import AnalysisException
    from pyspark.sql import functions as F

    silver = spark.read.parquet(silver_lake.silver("trades"))
    with pytest.raises(AnalysisException, match="window_start"):
        silver.groupBy("symbol", F.to_date("window_start").alias("day")).agg(
            F.max("high")
        ).collect()


def test_daily_summary_matches_reference(spark, silver_lake, reference) -> None:
    from batch.gold import DAILY_COLUMNS, build_daily
    from batch.silver import read_silver

    daily = _to_pandas(
        build_daily(
            read_silver(spark, silver_lake, "trades", FIRST_DAY, LAST_DAY),
            read_silver(spark, silver_lake, "bars", FIRST_DAY, LAST_DAY),
        )
    )
    daily["day"] = daily["day"].map(lambda value: value.isoformat())
    expected = reference["daily"].set_index(["symbol", "day"]).sort_index()
    actual = daily.set_index(["symbol", "day"]).sort_index()

    assert list(actual.index) == list(expected.index)
    assert set(actual["price_source"]) == {"trades"}
    assert actual["asset_class"].equals(expected["asset_class"])
    for column in DAILY_COLUMNS[3:]:
        for key in expected.index:
            assert _close(actual.at[key, column], expected.at[key, column]), (key, column)


def test_correlation_matches_reference(spark, silver_lake, reference) -> None:
    from batch.gold import build_gold

    gold = build_gold(spark, silver_lake, [LAST_DAY])
    actual = _to_pandas(gold["correlation_matrix"])
    expected = reference["correlation"]

    assert set(actual["dt"]) == {LAST_DAY}
    assert {value.isoformat() for value in actual["as_of"]} == set(expected["as_of"])
    actual_pairs = actual.set_index(["base_symbol", "quote_symbol"])
    expected_pairs = expected.set_index(["base_symbol", "quote_symbol"])
    assert set(actual_pairs.index) == set(expected_pairs.index)
    assert ("REAL10Y", "US10Y") in set(expected_pairs.index)
    for key in expected_pairs.index:
        assert abs(actual_pairs.at[key, "correlation"] - expected_pairs.at[key, "correlation"]) <= 1.5e-6, key
        assert actual_pairs.at[key, "n_obs"] == expected_pairs.at[key, "n_obs"], key


def test_yield_curve_matches_reference(spark, silver_lake, reference) -> None:
    from batch.gold import build_yield_curve
    from batch.silver import read_silver

    curve = build_yield_curve(read_silver(spark, silver_lake, "yields", FIRST_DAY, LAST_DAY))
    actual = _to_pandas(curve)
    actual["day"] = actual["day"].map(lambda value: value.isoformat())
    actual = actual.set_index("day").sort_index()
    expected = reference["curve"].set_index("day").sort_index()

    assert list(actual.index) == list(expected.index)
    assert set(actual["curve"]) == {"UST"}
    for column in expected.columns:
        for day in expected.index:
            if column == "inverted":
                assert bool(actual.at[day, column]) == bool(expected.at[day, column])
            else:
                assert _close(actual.at[day, column], expected.at[day, column]), (day, column)


def test_gold_partitions_and_ml_features(spark, silver_lake) -> None:
    from batch.gold import build_gold, read_gold, write_gold

    # Wed 2026-11-25 has stock, crypto and yield data; Sun 2026-11-29 has crypto only,
    # so its yield_curve partition is empty and must still read back cleanly.
    days = [date(2026, 11, 25), LAST_DAY]
    write_gold(silver_lake, build_gold(spark, silver_lake, days))

    for table in ("daily_summary", "correlation_matrix", "yield_curve", "ml_features"):
        written = read_gold(spark, silver_lake, table, days[0], days[-1])
        assert {row["dt"] for row in written.select("dt").distinct().collect()} <= set(days)
    curve_days = read_gold(spark, silver_lake, "yield_curve", days[0], days[-1]).collect()
    assert [row["day"] for row in curve_days] == [date(2026, 11, 25)]

    features = read_gold(spark, silver_lake, "ml_features", LAST_DAY, LAST_DAY)
    by_symbol = {row["symbol"]: row for row in features.collect()}
    assert set(by_symbol) == set(INSTRUMENTS)
    # Bond ETFs fall when the 10Y yield rises (synthetic market, PDF question 1).
    assert by_symbol["TLT"]["corr_us10y"] < -0.5
    assert by_symbol["BTCUSDT"]["volatility"] > by_symbol["SHY"]["volatility"]


def test_daily_source_precedence(spark) -> None:
    from batch.gold import build_daily

    trades = spark.createDataFrame(
        [
            ("a", "stocks.trades", "SPY", "equity", "alpaca", None, 1_788_273_000_000, 1, 590.0, 10.0, date(2026, 9, 1)),
            ("b", "stocks.trades", "SPY", "equity", "alpaca", None, 1_788_359_400_000, 1, 591.0, 10.0, date(2026, 9, 2)),
        ],
        "trade_id string, topic string, symbol string, asset_class string, source string, "
        "exchange_trade_id string, event_time_ms long, ingest_time long, price double, "
        "volume double, dt date",
    ).selectExpr("*", "timestamp_millis(event_time_ms) AS event_time")
    bars = spark.createDataFrame(
        [
            # yfinance daily bar for 2026-09-01 must win over the IEX trades of that day.
            ("d1", "SPY", "equity", "1Day", "yahoo", 1_788_220_800_000, 1, 589.0, 595.0, 585.0, 592.0, 9e7, None, None),
            # Minute bars only for 2026-09-03: used because no trades exist that day.
            ("m1", "SPY", "equity", "1Min", "alpaca", 1_788_445_800_000, 1, 593.0, 594.0, 592.0, 593.5, 100.0, 593.2, 5),
        ],
        "bar_id string, symbol string, asset_class string, timeframe string, source string, "
        "event_time_ms long, ingest_time long, open double, high double, low double, "
        "close double, volume double, vwap double, trade_count long",
    ).selectExpr("*", "timestamp_millis(event_time_ms) AS event_time")

    rows = {row["day"].isoformat(): row for row in build_daily(trades, bars).collect()}
    assert rows["2026-09-01"]["price_source"] == "bar_1d"
    assert rows["2026-09-01"]["close"] == 592.0
    assert rows["2026-09-02"]["price_source"] == "trades"
    assert rows["2026-09-03"]["price_source"] == "bar_1m"
    assert rows["2026-09-03"]["vwap"] == pytest.approx(593.2)
