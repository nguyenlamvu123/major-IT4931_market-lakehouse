"""Operator tools: exactly-once comparison and latency metrics rows."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from streaming.tools.exactly_once_check import compare_bars
from streaming.tools.measure_latency import metrics_row

SAMPLES = Path(__file__).resolve().parents[2] / "contracts" / "samples"


def _bars(counts: list[int], volumes: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["BTCUSDT"] * len(counts),
            "window_start": pd.date_range("2026-11-09T14:30:00Z", periods=len(counts), freq="1min"),
            "trade_count": counts,
            "volume": volumes,
        }
    )


def test_compare_bars_flags_double_counting_and_gaps() -> None:
    expected = _bars([2, 1, 3], [1.0, 2.0, 3.0])
    assert compare_bars(expected, expected.copy())["ok"]

    replayed = _bars([4, 1], [2.0, 2.0])  # first bar counted twice, last bar missing
    report = compare_bars(expected, replayed)
    assert not report["ok"]
    assert report["trade_count_mismatch"] == ["BTCUSDT@2026-11-09T14:30:00+00:00"]
    assert report["volume_mismatch"] == ["BTCUSDT@2026-11-09T14:30:00+00:00"]
    assert report["missing_in_sink"] == ["BTCUSDT@2026-11-09T14:32:00+00:00"]


def test_metrics_row_is_a_markdown_table_row() -> None:
    row = metrics_row(
        "ohlcv_1m",
        {"count": 120, "p50": 3.1, "p95": 6.4, "max": 9.0},
        "trigger 5s, 1 executor",
        datetime(2026, 11, 20, 14, 0, tzinfo=timezone.utc),
    )
    assert row == (
        "| 2026-11-20 14:00 UTC | latency trade->ohlcv_1m | trigger 5s, 1 executor "
        "| n=120 p50=3.1s p95=6.4s max=9.0s |\n"
    )


def test_replayed_micro_batch_is_not_double_counted(spark, tmp_path: Path) -> None:
    """Crash between the sink write and the checkpoint commit, then restart.

    Deleting ``commits/<last>`` is exactly the state a driver killed mid-batch leaves:
    the offsets are logged, the sink already has the rows, the commit is missing. On
    restart Spark re-runs that batch with the same offsets; the upsert semantics make
    the final table identical to a single run.
    """
    from streaming.job import SpeedLayer, parse_args, start_queries
    from streaming.sinks import ParquetSink
    from streaming.tools.exactly_once_check import main as check

    lines = (SAMPLES / "crypto.trades.jsonl").read_text(encoding="utf-8").splitlines()
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    for index, chunk in enumerate((lines[:10], lines[10:]), start=1):
        part = input_dir / f"crypto.trades-{index}.jsonl"
        part.write_text("\n".join(chunk) + "\n", encoding="utf-8")
        os.utime(part, (1_000_000_000 + index, 1_000_000_000 + index))
    out = tmp_path / "speed"
    checkpoints = tmp_path / "checkpoints"
    argv = [
        "--source",
        "files",
        "--input",
        str(input_dir),
        "--available-now",
        "--max-files-per-trigger",
        "1",
        "--queries",
        "speed-ohlcv-1m",
        "--checkpoint-root",
        str(checkpoints),
        "--output-mode",
        "parquet",
        "--parquet-path",
        str(out),
    ]

    def run() -> None:
        args = parse_args(argv)
        for query in start_queries(spark, args, SpeedLayer(spark, ParquetSink(str(out)))):
            query.awaitTermination(180)
            assert query.exception() is None

    # Without this, Spark appends an empty "no-data" batch to advance the watermark and
    # that empty batch would be the one replayed.
    spark.conf.set("spark.sql.streaming.noDataMicroBatches.enabled", "false")
    try:
        run()
    finally:
        spark.conf.unset("spark.sql.streaming.noDataMicroBatches.enabled")
    commits = checkpoints / "speed-ohlcv-1m" / "commits"
    last = max(int(path.name) for path in commits.iterdir() if path.name.isdigit())
    assert last >= 1
    written_before = len(pd.read_parquet(out / "ohlcv_1m"))
    # A crash before the commit leaves neither the commit file nor the checksum file
    # that Hadoop's local filesystem writes next to it.
    (commits / str(last)).unlink()
    (commits / f".{last}.crc").unlink(missing_ok=True)
    run()

    raw = pd.read_parquet(out / "ohlcv_1m")
    assert len(raw) > written_before  # the last batch really was replayed
    report = check(["--trades", str(input_dir / "*.jsonl"), "--parquet-root", str(out)])
    assert report["ok"], json.dumps(report)
    assert report["expected_bars"] == report["actual_bars"]


def test_check_cli_fails_on_mismatch(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    from streaming.tools.exactly_once_check import main as check

    bars = _bars([5], [9.0]).assign(updated_at=pd.Timestamp("2026-11-09T14:31:00Z"))
    (tmp_path / "ohlcv_1m").mkdir()
    bars.to_parquet(tmp_path / "ohlcv_1m" / "part.parquet")
    with pytest.raises(SystemExit):
        check(["--trades", str(SAMPLES / "crypto.trades.jsonl"), "--parquet-root", str(tmp_path)])
