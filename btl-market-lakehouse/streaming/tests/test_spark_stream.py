"""End-to-end speed layer on a local SparkSession, fed by the contract sample files."""

from __future__ import annotations

import json
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
    from streaming.job import SpeedLayer, parse_args, start_query

    args = parse_args(
        [
            "--source",
            "files",
            "--input",
            str(input_dir),
            "--max-files-per-trigger",
            "4",
            "--checkpoint-root",
            str(tmp_path / "checkpoints"),
            "--available-now",
            *(extra or []),
        ]
    )
    query = start_query(spark, args, SpeedLayer(spark, sink))
    query.awaitTermination(180)
    assert query.exception() is None
    return args


def test_file_mode_bars_match_pandas_reference(spark, memory_sink, input_dir, tmp_path) -> None:
    _run(spark, input_dir, tmp_path, memory_sink)
    bars = memory_sink.table("ohlcv_1m")
    assert not bars.empty
    assert bars["symbol"].isin(["BTCUSDT", "AAPL", "GC=F", "DXY"]).any()
    # Non-trade sample files (bars, yields, alerts) never become trades.
    assert bars["trade_count"].sum() <= len(_sample_trades())

    reference = aggregate_trades(_sample_trades(), "1min").set_index(["symbol", "window_start"])
    latest = (
        bars.assign(window_start=pd.to_datetime(bars["window_start"], utc=True))
        .sort_values("updated_at")
        .groupby(["symbol", "window_start"])
        .tail(1)
        .set_index(["symbol", "window_start"])
    )
    for key, row in latest.iterrows():
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
        assert row["window_end"] == (key[1] + pd.Timedelta(minutes=1)).tz_localize(None)
        assert row["asset_class"] == expected["asset_class"]


def test_dxy_bars_have_null_vwap(spark, memory_sink, input_dir, tmp_path) -> None:
    _run(spark, input_dir, tmp_path, memory_sink)
    bars = memory_sink.table("ohlcv_1m")
    dxy = bars[bars["symbol"] == "DXY"]
    assert not dxy.empty
    assert dxy["vwap"].isna().all()
    assert (dxy["volume"] == 0).all()
