"""End-to-end speed layer on a local SparkSession, fed by the contract sample files."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pandas as pd
import pytest

from common.ohlcv import aggregate_trades

SAMPLES = Path(__file__).resolve().parents[2] / "contracts" / "samples"
TRADE_TOPICS = ["crypto.trades", "stocks.trades", "gold.quotes", "fx.dxy"]


def _sample_trades() -> pd.DataFrame:
    rows = []
    for topic in TRADE_TOPICS:
        for line in (SAMPLES / f"{topic}.jsonl").read_text(encoding="utf-8").splitlines():
            rows.append(json.loads(line))
    return pd.DataFrame(rows)


@pytest.fixture
def input_dir(tmp_path: Path) -> Path:
    target = tmp_path / "input"
    target.mkdir()
    # Copy every sample, including non-trade topics: the file source must ignore them.
    for path in SAMPLES.glob("*.jsonl"):
        shutil.copy(path, target / path.name)
    # A producer reconnect re-sends the first crypto trade: same trade_id, same content.
    first = (SAMPLES / "crypto.trades.jsonl").read_text(encoding="utf-8").splitlines()[0]
    with (target / "crypto.trades.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(first + "\n")
    return target


def _run(spark, input_dir: Path, tmp_path: Path, sink, extra: list[str] | None = None):
    from streaming.job import SpeedLayer, parse_args, start_queries

    args = parse_args(
        [
            "--source",
            "files",
            "--input",
            str(input_dir),
            "--checkpoint-root",
            str(tmp_path / "checkpoints"),
            "--available-now",
            *(extra or ["--max-files-per-trigger", "4"]),
        ]
    )
    for query in start_queries(spark, args, SpeedLayer(spark, sink)):
        query.awaitTermination(180)
        assert query.exception() is None
    return args


def _final_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Latest upsert per primary key, i.e. what Cassandra would hold."""
    return (
        frame.assign(window_start=pd.to_datetime(frame["window_start"], utc=True))
        .sort_values("updated_at", kind="stable")
        .groupby(["symbol", "window_start"])
        .tail(1)
        .set_index(["symbol", "window_start"])
        .sort_index()
    )


def _assert_bars_match(actual: pd.DataFrame, reference: pd.DataFrame, minutes: int) -> None:
    assert list(actual.index) == list(reference.index)
    for key, row in actual.iterrows():
        expected = reference.loc[key]
        assert row["open"] == expected["open"], key
        assert row["high"] == expected["high"], key
        assert row["low"] == expected["low"], key
        assert row["close"] == expected["close"], key
        # The duplicated crypto trade must not be counted twice.
        assert row["volume"] == pytest.approx(expected["volume"]), key
        assert row["trade_count"] == expected["trade_count"], key
        if pd.isna(expected["vwap"]):
            assert pd.isna(row["vwap"]), key
        else:
            assert row["vwap"] == pytest.approx(expected["vwap"]), key
        assert row["day"] == key[1].date()
        assert row["window_end"] == (key[1] + pd.Timedelta(minutes=minutes)).tz_localize(None)
        assert row["asset_class"] == expected["asset_class"]


def test_update_mode_emits_every_bar_including_open_ones(
    spark, memory_sink, input_dir, tmp_path
) -> None:
    _run(spark, input_dir, tmp_path, memory_sink)
    trades = _sample_trades()

    one_minute = _final_rows(memory_sink.table("ohlcv_1m"))
    reference_1m = aggregate_trades(trades, "1min").set_index(["symbol", "window_start"])
    # Append mode would hold back the bars within 30 s of the newest trade; update mode
    # writes all of them, so the final table equals the batch reference exactly.
    _assert_bars_match(one_minute, reference_1m.sort_index(), minutes=1)

    five_minutes = _final_rows(memory_sink.table("ohlcv_5m"))
    reference_5m = aggregate_trades(trades, "5min").set_index(["symbol", "window_start"])
    _assert_bars_match(five_minutes, reference_5m.sort_index(), minutes=5)


def test_partial_bar_is_upserted_when_more_trades_arrive(spark, memory_sink, tmp_path) -> None:
    lines = (SAMPLES / "crypto.trades.jsonl").read_text(encoding="utf-8").splitlines()
    btc = [line for line in lines if json.loads(line)["symbol"] == "BTCUSDT"]
    assert btc, "sample must contain a BTC trade"
    # Split the first BTC minute: the opening trade arrives alone, a later trade of the
    # same minute arrives in the next micro-batch.
    opening = json.loads(btc[0])
    later = dict(
        opening,
        trade_id="f" * 16,
        event_time=opening["event_time"] + 30_000,
        price=opening["price"] * 1.01,
        volume=2.0,
    )
    input_dir = tmp_path / "split"
    input_dir.mkdir()
    part_one = input_dir / "crypto.trades-1.jsonl"
    part_two = input_dir / "crypto.trades-2.jsonl"
    part_one.write_text(json.dumps(opening) + "\n", encoding="utf-8")
    part_two.write_text(json.dumps(later) + "\n", encoding="utf-8")
    # The file source orders files by modification time.
    os.utime(part_one, (1_000_000_000, 1_000_000_000))
    os.utime(part_two, (1_000_000_100, 1_000_000_100))

    _run(
        spark,
        input_dir,
        tmp_path,
        memory_sink,
        ["--max-files-per-trigger", "1", "--queries", "speed-ohlcv-1m"],
    )
    writes = memory_sink.table("ohlcv_1m")
    first_bar = writes[writes["symbol"] == "BTCUSDT"].sort_values("updated_at")
    assert len(first_bar) == 2  # written as soon as it opened, then upserted
    assert first_bar.iloc[0]["trade_count"] == 1
    final = first_bar.iloc[-1]
    assert final["trade_count"] == 2
    assert final["open"] == opening["price"]
    assert final["close"] == later["price"]
    assert final["volume"] == pytest.approx(opening["volume"] + 2.0)


def test_dxy_bars_have_null_vwap(spark, memory_sink, input_dir, tmp_path) -> None:
    _run(spark, input_dir, tmp_path, memory_sink)
    bars = memory_sink.table("ohlcv_1m")
    dxy = bars[bars["symbol"] == "DXY"]
    assert not dxy.empty
    assert dxy["vwap"].isna().all()
    assert (dxy["volume"] == 0).all()


def test_spike_across_micro_batches_reaches_alert_and_indicator_tables(
    spark, memory_sink, tmp_path
) -> None:
    from common.contracts import make_trade

    start = pd.Timestamp("2026-11-09T14:30:00Z")

    def trade(minute: int, price: float) -> str:
        event_time = int((start + pd.Timedelta(minutes=minute, seconds=5)).timestamp() * 1000)
        record = make_trade(
            "SPY", "equity", event_time, price, 10.0, "replay", exchange_trade_id=f"spy-{minute}"
        )
        return json.dumps(record)

    input_dir = tmp_path / "spike"
    input_dir.mkdir()
    calm = input_dir / "stocks.trades-1.jsonl"
    spike = input_dir / "stocks.trades-2.jsonl"
    calm.write_text("\n".join(trade(m, 100.0 + m % 2) for m in range(30)) + "\n", encoding="utf-8")
    spike.write_text(trade(30, 110.0) + "\n", encoding="utf-8")
    os.utime(calm, (1_000_000_000, 1_000_000_000))
    os.utime(spike, (1_000_000_100, 1_000_000_100))

    _run(
        spark,
        input_dir,
        tmp_path,
        memory_sink,
        ["--max-files-per-trigger", "1", "--queries", "speed-ohlcv-1m"],
    )

    alerts = memory_sink.table("alerts_by_day")
    assert list(alerts["alert_type"]) == ["PRICE_SPIKE"]
    assert alerts.iloc[0]["symbol"] == "SPY"
    assert pd.Timestamp(alerts.iloc[0]["alert_time"]) == (
        start + pd.Timedelta(minutes=30)
    ).tz_localize(None)
    assert str(alerts.iloc[0]["day"]) == "2026-11-09"
    indicators = memory_sink.table("indicators_latest")
    newest = indicators.sort_values("updated_at").iloc[-1]
    assert newest["symbol"] == "SPY"
    assert newest["close"] == 110.0
    assert newest["zscore_30m"] > 10
