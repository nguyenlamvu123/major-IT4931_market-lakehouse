#!/usr/bin/env python3
"""Before/after measurements for the batch layer (PDF task C.6: "Tối ưu hiệu năng có số
liệu trước/sau: số partition, broadcast join, cache, AQE").

Call stack:

main
  -> generate_silver           Spark-native synthetic silver trades + yields (millions of rows
                               without going through Python dicts)
  -> measure(...) per variant  same computation, one Spark setting flipped, written to the
                               "noop" sink so the timing covers compute, not disk output
       -> batch.gold.daily_from_trades   partition pruning on dt vs filter on event_time
       -> batch.gold.build_gold          AQE, shuffle partitions, broadcast threshold, cache
  -> write a Markdown table    --report docs/metrics/C-batch.md

One shared warm-up run, then each variant is timed ``--repeat`` times (median).
Numbers from a laptop in local mode only show the direction and rough size of each
effect; re-measure on the cluster (Spark Operator, MinIO) for the report.

Example::

    python batch/jobs/benchmark.py --lake file:///tmp/bench --days 365 --rows-per-day 3000 \
        --master 'local[2]' --report docs/metrics/C-batch.md
"""

from __future__ import annotations

import argparse
import os
import platform
import statistics
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from batch.gold import build_gold, daily_from_trades  # noqa: E402
from batch.lake import Lake  # noqa: E402
from batch.silver import SILVER_DDL  # noqa: E402
from batch.spark_session import build_spark  # noqa: E402
from common.contracts import INSTRUMENTS, synthetic_yields  # noqa: E402


@dataclass(frozen=True)
class Measurement:
    technique: str
    before_label: str
    before_seconds: float
    after_label: str
    after_seconds: float
    plan_note: str

    @property
    def speedup(self) -> float:
        return self.before_seconds / self.after_seconds if self.after_seconds else float("nan")


def generate_silver(spark: SparkSession, lake: Lake, end_day: date, days: int, rows_per_day: int) -> int:
    """Write ``days`` of synthetic silver trades and yields ending at ``end_day``.

    Stock-calendar symbols skip weekends so the SPY calendar logic does real work.
    Returns the number of trade rows written.
    """
    symbols = list(INSTRUMENTS)
    first_day = end_day - timedelta(days=days - 1)
    start_ms = int(datetime.combine(first_day, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)

    def mapping(values: dict[str, object]):
        return F.create_map(*[item for pair in values.items() for item in (F.lit(pair[0]), F.lit(pair[1]))])

    by_index = {str(index): symbol for index, symbol in enumerate(symbols)}
    base_price = mapping({symbol: INSTRUMENTS[symbol].base_price for symbol in symbols})
    asset_class = mapping({symbol: INSTRUMENTS[symbol].asset_class for symbol in symbols})
    topic = mapping({symbol: INSTRUMENTS[symbol].topic for symbol in symbols})
    symbol_of = mapping(by_index)

    trades = (
        spark.range(days * rows_per_day)
        .withColumn("day_index", F.col("id") % days)
        .withColumn("symbol", symbol_of[((F.col("id") / days).cast("long") % len(symbols)).cast("string")])
        .withColumn("seq", (F.col("id") / (days * len(symbols))).cast("long"))
        .withColumn(
            "event_time_ms",
            F.lit(start_ms) + F.col("day_index") * 86_400_000 + 52_200_000 + F.col("seq") * 1_000,
        )
        .withColumn("event_time", F.timestamp_millis("event_time_ms"))
        .withColumn("dt", F.to_date("event_time"))
        .withColumn("asset_class", asset_class[F.col("symbol")])
        .withColumn("topic", topic[F.col("symbol")])
        # dayofweek: 1 = Sunday, 7 = Saturday. Only crypto trades on weekends.
        .filter((F.col("asset_class") == "crypto") | ~F.dayofweek("dt").isin(1, 7))
        .withColumn(
            "price",
            base_price[F.col("symbol")]
            * (1 + 0.05 * F.sin(F.col("day_index") / 9.0 + F.col("id") % 7) + 0.002 * F.randn(7)),
        )
        .withColumn("volume", F.rand(11) * 10)
        .withColumn("trade_id", F.substring(F.md5(F.col("id").cast("string")), 1, 16))
        .withColumn("source", F.lit("bench"))
        .withColumn("exchange_trade_id", F.col("id").cast("string"))
        .withColumn("ingest_time", F.col("event_time_ms") + 50)
    )
    columns = [item.split()[0] for item in SILVER_DDL["trades"].split(", ")]
    trades.select(columns).write.mode("overwrite").partitionBy("dt").parquet(lake.silver("trades"))

    yields = [
        (row["series_id"], row["tenor"], date.fromisoformat(row["day"]), row["yield"])
        for row in synthetic_yields(days=days, seed=42, start=datetime.combine(first_day, datetime.min.time(), tzinfo=timezone.utc))
    ]
    (
        spark.createDataFrame(yields, "series_id string, tenor string, day date, yield double")
        .withColumn("event_time", F.to_timestamp("day"))
        .withColumn("source", F.lit("bench"))
        .withColumn("ingest_time", F.lit(None).cast("long"))
        .withColumn("dt", F.col("day"))
        .write.mode("overwrite")
        .partitionBy("dt")
        .parquet(lake.silver("yields"))
    )
    return spark.read.parquet(lake.silver("trades")).count()


def _plan(frame: DataFrame) -> str:
    return frame.sparkSession._jvm.PythonSQLUtils.explainString(
        frame._jdf.queryExecution(), "simple"
    )


def _time(action: Callable[[], None], repeat: int) -> float:
    samples = []
    for _ in range(repeat):
        started = time.perf_counter()
        action()
        samples.append(time.perf_counter() - started)
    return statistics.median(samples)


def _write_noop(frame: DataFrame) -> None:
    frame.write.format("noop").mode("overwrite").save()


def run(spark: SparkSession, lake: Lake, day: date, repeat: int) -> list[Measurement]:
    """Time each technique; a configuration shared by two comparisons is measured once."""
    lookback_start = day - timedelta(days=150)
    trades_path = lake.silver("trades")
    baseline = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.shuffle.partitions": "8",
        "spark.sql.autoBroadcastJoinThreshold": str(10 * 1024 * 1024),
    }

    def configure(**overrides: str) -> None:
        spark.catalog.clearCache()
        for key, value in {**baseline, **overrides}.items():
            spark.conf.set(key, value)

    def gold(cache: bool = True) -> Callable[[], None]:
        def action() -> None:
            spark.catalog.clearCache()
            for frame in build_gold(spark, lake, [day], cache=cache).values():
                _write_noop(frame)

        return action

    pruned = spark.read.parquet(trades_path).filter(
        F.col("dt").between(F.lit(lookback_start), F.lit(day))
    )
    scanned = spark.read.parquet(trades_path).filter(
        F.to_date("event_time").between(F.lit(lookback_start), F.lit(day))
    )

    # One warm-up for the whole run (JIT, Python workers, Parquet footer cache), so the
    # first measured variant is not penalised.
    configure()
    gold()()
    _write_noop(daily_from_trades(pruned))

    no_pruning = _time(lambda: _write_noop(daily_from_trades(scanned)), repeat)
    pruning = _time(lambda: _write_noop(daily_from_trades(pruned)), repeat)
    pruning_note = (
        "PartitionFilters: [dt >= …, dt <= …]"
        if "PartitionFilters: [isnotnull(dt" in _plan(pruned)
        else "không thấy PartitionFilters"
    )

    configure(**{"spark.sql.adaptive.enabled": "false", "spark.sql.shuffle.partitions": "200"})
    partitions_200 = _time(gold(), repeat)
    configure(**{"spark.sql.adaptive.enabled": "false"})
    partitions_8 = _time(gold(), repeat)
    broadcast_plan = _plan(build_gold(spark, lake, [day])["correlation_matrix"])
    configure(**{"spark.sql.adaptive.enabled": "true", "spark.sql.shuffle.partitions": "200"})
    aqe_200 = _time(gold(), repeat)
    configure(**{"spark.sql.adaptive.enabled": "false", "spark.sql.autoBroadcastJoinThreshold": "-1"})
    sort_merge_plan = _plan(build_gold(spark, lake, [day])["correlation_matrix"])
    no_broadcast = _time(gold(), repeat)
    configure()
    no_cache = _time(gold(cache=False), repeat)
    with_cache = _time(gold(cache=True), repeat)
    configure()

    return [
        Measurement(
            "Partition pruning theo dt",
            "lọc to_date(event_time) (quét mọi dt)",
            no_pruning,
            "lọc cột phân vùng dt",
            pruning,
            pruning_note,
        ),
        Measurement(
            "Số shuffle partition (AQE tắt)", "200 (mặc định)", partitions_200, "8", partitions_8,
            "200 task gần rỗng mỗi stage trên local[2]",
        ),
        Measurement(
            "AQE (200 partition)", "tắt", partitions_200, "bật", aqe_200,
            "AQE gộp partition nhỏ sau shuffle (coalescePartitions)",
        ),
        Measurement(
            "Broadcast join bảng nhỏ (AQE tắt, 8 partition)",
            "tắt (autoBroadcastJoinThreshold=-1)",
            no_broadcast,
            "bật (10 MB)",
            partitions_8,
            f"SortMergeJoin {sort_merge_plan.count('SortMergeJoin')} → "
            f"{broadcast_plan.count('SortMergeJoin')}, BroadcastHashJoin "
            f"{sort_merge_plan.count('BroadcastHashJoin')} → {broadcast_plan.count('BroadcastHashJoin')}",
        ),
        Measurement(
            "Cache DataFrame dùng lại", "không cache", no_cache, "cache daily/changes/corr",
            with_cache, "daily_from_trades chạy 1 lần thay vì mỗi bảng gold 1 lần",
        ),
    ]


def render(results: list[Measurement], *, rows: int, days: int, day: date, master: str, repeat: int) -> str:
    lines = [
        "| Kỹ thuật | Trước | Giây | Sau | Giây | Tăng tốc | Ghi chú (plan) |",
        "|---|---|---:|---|---:|---:|---|",
    ]
    for item in results:
        lines.append(
            f"| {item.technique} | {item.before_label} | {item.before_seconds:.2f} | "
            f"{item.after_label} | {item.after_seconds:.2f} | x{item.speedup:.2f} | {item.plan_note} |"
        )
    header = (
        f"- Ngày đo: {datetime.now(timezone.utc).date().isoformat()}\n"
        f"- Máy: {platform.system()} {platform.machine()}, {os.cpu_count()} CPU, Python "
        f"{platform.python_version()}, Spark {SparkSession.getActiveSession().version}, "
        f"master `{master}`, driver 1 GB\n"
        f"- Dữ liệu: {rows:,} dòng silver trades giả lập trong {days} ngày (12 mã), "
        f"tính gold cho dt={day} với lookback 150 ngày\n"
        f"- 1 lần khởi động chung, mỗi biến thể lấy trung vị của {repeat} lần đo, ghi vào sink `noop`\n"
    )
    return header + "\n" + "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch layer before/after benchmark")
    parser.add_argument("--lake", required=True, help="scratch lake root, e.g. file:///tmp/bench")
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--rows-per-day", type=int, default=3_000)
    parser.add_argument("--end-day", type=date.fromisoformat, default=date(2026, 11, 25))
    parser.add_argument("--repeat", type=int, default=2)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--report", default="", help="write the Markdown table to this file")
    args = parser.parse_args()

    spark = build_spark(
        "batch-benchmark",
        master=args.master,
        extra_conf={"spark.driver.memory": "1g", "spark.ui.enabled": "false"},
    )
    spark.sparkContext.setLogLevel("ERROR")
    lake = Lake(args.lake)
    rows = generate_silver(spark, lake, args.end_day, args.days, args.rows_per_day)
    results = run(spark, lake, args.end_day, args.repeat)
    table = render(results, rows=rows, days=args.days, day=args.end_day, master=args.master,
                   repeat=args.repeat)
    print(table)
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(table, encoding="utf-8")


if __name__ == "__main__":
    main()
