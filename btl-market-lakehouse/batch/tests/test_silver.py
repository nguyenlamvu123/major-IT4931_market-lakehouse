"""Bronze -> silver on a local lake laid out like the Kafka Connect S3 sink output."""

from __future__ import annotations

import json
from datetime import date

import pytest

from common.alerts import make_alert
from common.contracts import (
    INSTRUMENTS,
    TRADE_TOPICS,
    make_bar,
    make_rate,
    make_trade,
    synthetic_trades,
)

DAY1 = date(2026, 9, 1)
DAY2 = date(2026, 9, 2)


def _ms(value: str) -> int:
    from datetime import datetime

    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


@pytest.fixture()
def bronze(lake_dir, bronze_writer):
    """Two days of every topic, plus duplicates and contract violations."""
    trades = synthetic_trades(rows=600, seed=3, days=2)
    for topic in TRADE_TOPICS:
        records = [record for record in trades if INSTRUMENTS[record["symbol"]].topic == topic]
        extra = {}
        if topic == "crypto.trades":
            records.append(dict(records[0]))  # re-sent after a reconnect
            records.append(dict(records[1], trade_id="badprice0000000", price=-5.0))
            records.append(dict(records[2], trade_id="badclass0000000", asset_class="rates"))
            # Event on DAY2 that ended up in the DAY1 partition: must not leak into DAY2.
            records.append(
                make_trade("BTCUSDT", "crypto", _ms("2026-09-02T01:00:00Z"), 1.0, 1.0, "demo",
                           exchange_trade_id="late")
            )
            extra = {DAY1.isoformat(): ["{not json"]}
        bronze_writer(lake_dir, topic, records, extra)
    # The misplaced record above is written under its own event day by write_bronze;
    # move it into DAY1's file to simulate a mis-partitioned sink.
    crypto_day2 = lake_dir / "bronze" / "crypto.trades" / f"dt={DAY2}" / "crypto.trades+0+0000000000.json"
    lines = crypto_day2.read_text().splitlines()
    misplaced = [line for line in lines if '"late"' in line]
    crypto_day2.write_text("\n".join(line for line in lines if '"late"' not in line) + "\n")
    crypto_day1 = lake_dir / "bronze" / "crypto.trades" / f"dt={DAY1}" / "crypto.trades+0+0000000000.json"
    crypto_day1.write_text(crypto_day1.read_text() + "\n".join(misplaced) + "\n")

    bars = [
        make_bar("SPY", "equity", "1Min", _ms("2026-09-01T14:30:00Z"), 590, 591, 589, 590.5, 100, "alpaca"),
        make_bar("SPY", "equity", "1Min", _ms("2026-09-01T14:31:00Z"), 590, 589, 588, 590.5, 100, "alpaca"),
        make_bar("SPY", "equity", "1Day", _ms("2026-09-02T00:00:00Z"), 590, 595, 585, 592, 9e7, "yahoo"),
    ]
    bronze_writer(lake_dir, "stocks.bars", bars + [bars[0]])

    old = make_rate("DGS10", DAY1.isoformat(), 4.10, "fred", ingest_time=1)
    revised = make_rate("DGS10", DAY1.isoformat(), 4.12, "fred", ingest_time=2)
    wrong_tenor = dict(make_rate("DGS2", DAY1.isoformat(), 4.0, "fred"), tenor="10Y")
    bronze_writer(lake_dir, "rates.yields", [old, revised, wrong_tenor])

    alert = make_alert("BTCUSDT", "2026-09-01T15:00:00+00:00", "PRICE_SPIKE", "high", 1.0, "x")
    bronze_writer(lake_dir, "alerts", [alert, alert, dict(alert, alert_id="b" * 16, severity="?")])
    return lake_dir


def test_reading_bronze_root_in_one_call_fails(spark, bronze) -> None:
    # ROOT CAUSE:
    #
    # The v1 job read the whole bronze root with spark.read.json("s3a://lake/bronze").
    # Kafka Connect writes bronze/<topic>/dt=<day>/, so partition discovery finds one base
    # path per topic and Spark aborts:
    #
    #   spark.read.schema(TRADE_SCHEMA).json(bronze_root)
    #   -> AssertionError: Conflicting directory structures detected
    #
    # We fixed this by reading each topic's dt=<day>/ directories separately
    # (batch.silver.read_bronze) and keeping the partition in _bronze_dt.
    from batch.schemas import TRADE_SCHEMA

    with pytest.raises(Exception, match="Conflicting directory structures"):
        spark.read.schema(TRADE_SCHEMA).json(str(bronze / "bronze")).count()


def test_build_silver_validates_and_dedupes(spark, bronze) -> None:
    from batch.lake import Lake
    from batch.silver import build_silver

    tables, stats = build_silver(spark, Lake(bronze.as_uri()), [DAY1, DAY2])

    crypto_day1 = next(s for s in stats if s.topic == "crypto.trades" and s.dt == "2026-09-01")
    assert crypto_day1.rejected_by_reason == {
        "bad_asset_class": 1,
        "bad_price": 1,
        "dt_mismatch": 1,
        "malformed_json": 1,
    }
    assert crypto_day1.duplicate_rows == 1
    assert crypto_day1.input_rows == crypto_day1.output_rows + 5

    trades = tables["trades"]
    assert trades.count() == trades.select("trade_id").distinct().count()
    assert {row["topic"] for row in trades.select("topic").distinct().collect()} == set(TRADE_TOPICS)
    gld = trades.filter("symbol = 'GLD'").select("topic", "asset_class").first()
    assert (gld["topic"], gld["asset_class"]) == ("stocks.trades", "gold")
    days = {row["dt"] for row in trades.select("dt").distinct().collect()}
    assert days == {DAY1, DAY2}
    assert trades.filter("exchange_trade_id = 'late'").count() == 0

    bars = tables["bars"]
    assert bars.count() == 2  # one bad high/low rejected, one duplicate removed
    bar_stats = next(s for s in stats if s.topic == "stocks.bars" and s.dt == "2026-09-01")
    assert bar_stats.rejected_by_reason == {"bad_high_low": 1}

    yields = tables["yields"].collect()
    assert len(yields) == 1
    assert yields[0]["yield"] == 4.12  # FRED revision ingested last wins

    alerts = tables["alerts"].collect()
    assert len(alerts) == 1
    assert alerts[0]["day"] == DAY1


def test_rerunning_one_day_only_replaces_that_day(spark, bronze) -> None:
    from batch.lake import Lake
    from batch.silver import build_silver, write_silver

    lake = Lake(bronze.as_uri())
    tables, stats = build_silver(spark, lake, [DAY1, DAY2])
    write_silver(spark, lake, tables, stats)
    before = spark.read.parquet(lake.silver("trades")).groupBy("dt").count().collect()

    tables, stats = build_silver(spark, lake, [DAY1])
    write_silver(spark, lake, tables, stats)
    after = spark.read.parquet(lake.silver("trades")).groupBy("dt").count().collect()

    assert sorted(map(tuple, before)) == sorted(map(tuple, after))
    stats_rows = spark.read.json(lake.silver("_stats")).collect()
    assert {row["topic"] for row in stats_rows} >= set(TRADE_TOPICS)


def test_missing_bronze_day_is_not_an_error(spark, bronze) -> None:
    from batch.lake import Lake
    from batch.silver import build_silver

    tables, stats = build_silver(spark, Lake(bronze.as_uri()), [date(2026, 9, 6)])
    assert all(frame.count() == 0 for frame in tables.values())
    assert stats == []


def test_stats_are_json_serialisable(spark, bronze) -> None:
    from dataclasses import asdict

    from batch.lake import Lake
    from batch.silver import build_silver

    _, stats = build_silver(spark, Lake(bronze.as_uri()), [DAY1])
    assert json.loads(json.dumps([asdict(item) for item in stats]))
