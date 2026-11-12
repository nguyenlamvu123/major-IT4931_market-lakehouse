#!/usr/bin/env python3
"""Measure speed-layer latency locally, without Kafka or Cassandra.

Runs the real job (update mode, processing-time trigger) on the file source while a
feeder thread drops a JSONL file of trades stamped "now" every ``--feed-interval``
seconds. Afterwards it reports trade -> sink latency from the Parquet output and the
throughput Spark reported. The file source adds directory listing to the path, so the
numbers are an upper bound for the Kafka source with the same trigger.

    JAVA_HOME=... python -m streaming.tools.local_latency_run --seconds 60
"""

from __future__ import annotations

import argparse
import json
import random
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from common.contracts import INSTRUMENTS, make_trade  # noqa: E402
from streaming.metrics import latency_seconds, latency_stats  # noqa: E402


def feed(directory: Path, stop: threading.Event, interval: float, per_file: int) -> int:
    """Write ``crypto.trades-<n>.jsonl`` files with trades stamped at write time."""
    rng = random.Random(7)
    prices = {symbol: item.base_price for symbol, item in INSTRUMENTS.items()}
    symbols = [symbol for symbol, item in INSTRUMENTS.items() if item.topic == "crypto.trades"]
    sequence = 0
    while not stop.is_set():
        lines = []
        for _ in range(per_file):
            symbol = rng.choice(symbols)
            prices[symbol] *= 1 + rng.gauss(0, 0.0005)
            now_ms = int(time.time() * 1000)
            record = make_trade(
                symbol,
                "crypto",
                now_ms,
                round(prices[symbol], 4),
                round(rng.uniform(0.001, 0.5), 6),
                "replay",
                exchange_trade_id=f"local-{sequence}",
            )
            lines.append(json.dumps(record))
            sequence += 1
        # Spark ignores names starting with "_"; rename makes the file appear complete.
        temp = directory / f"_crypto.trades-{sequence}.tmp"
        temp.write_text("\n".join(lines) + "\n", encoding="utf-8")
        temp.rename(directory / f"crypto.trades-{sequence}.jsonl")
        stop.wait(interval)
    return sequence


def main(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--trigger", default="5 seconds")
    parser.add_argument("--feed-interval", type=float, default=1.0)
    parser.add_argument("--per-file", type=int, default=200)
    parser.add_argument("--keep", action="store_true", help="keep the temporary work dir")
    options = parser.parse_args(argv)

    from pyspark.sql import SparkSession

    from streaming.job import SpeedLayer, parse_args, start_queries
    from streaming.sinks import ParquetSink

    work = Path(tempfile.mkdtemp(prefix="speed-latency-"))
    inbox, out = work / "inbox", work / "speed"
    inbox.mkdir()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("speed-layer-local-latency")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    args = parse_args(
        [
            "--source",
            "files",
            "--input",
            str(inbox),
            "--trigger",
            options.trigger,
            "--max-files-per-trigger",
            "1000",
            "--output-mode",
            "parquet",
            "--parquet-path",
            str(out),
            "--checkpoint-root",
            str(work / "checkpoints"),
            "--metrics-port",
            "0",
        ]
    )
    queries = start_queries(spark, args, SpeedLayer(spark, ParquetSink(str(out))))
    stop = threading.Event()
    feeder = threading.Thread(
        target=feed, args=(inbox, stop, options.feed_interval, options.per_file)
    )
    feeder.start()
    time.sleep(options.seconds)
    stop.set()
    feeder.join()
    time.sleep(10)  # let the last trigger pick up the final file
    progress = {
        query.name: [p for p in query.recentProgress if p["numInputRows"]] for query in queries
    }
    for query in queries:
        query.stop()

    report = {
        "seconds": options.seconds,
        "trigger": options.trigger,
        "trades_per_second_fed": options.per_file / options.feed_interval,
        "workdir": str(work),
    }
    for table in ("ohlcv_1m", "ohlcv_5m"):
        report[f"latency_{table}"] = latency_stats(latency_seconds(_all_versions(out / table)))
    for name, batches in progress.items():
        rates = [p["processedRowsPerSecond"] for p in batches]
        durations = [p["durationMs"]["triggerExecution"] for p in batches]
        report[f"batches_{name}"] = {
            "count": len(batches),
            "processed_rows_per_second_avg": round(sum(rates) / len(rates), 1) if rates else None,
            "batch_duration_ms_max": max(durations) if durations else None,
        }
    spark.stop()
    if not options.keep:
        shutil.rmtree(work, ignore_errors=True)
    print(json.dumps(report, indent=2))
    return report


def _all_versions(path: Path):
    # Every upsert counts: each version's latency is what a dashboard saw at that time.
    import pandas as pd

    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


if __name__ == "__main__":
    main()
