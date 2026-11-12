#!/usr/bin/env python3
"""Generate 20 deterministic sample records per Kafka topic under contracts/samples.

The PDF asks role A to ship "mỗi topic 20 dòng mẫu trong contracts/samples/" so B and C
can code against real shapes before live data exists. Every record is validated against
the contract helpers before it is written; an invalid sample aborts the script.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.alerts import make_alert  # noqa: E402
from common.contracts import (  # noqa: E402
    CONTRACT_VERSION,
    INSTRUMENTS,
    TRADE_TOPICS,
    make_bar,
    synthetic_trades,
    synthetic_yields,
    validate_bar,
    validate_rate,
    validate_trade,
)
from common.io_utils import ensure_dir, write_json, write_jsonl  # noqa: E402
from common.ohlcv import aggregate_trades  # noqa: E402

SAMPLES = ROOT / "contracts" / "samples"
ROWS_PER_TOPIC = 20
SEED = 11


def _check(records: list[dict], validator, topic: str) -> list[dict]:
    for record in records:
        errors = validator(record)
        if errors:
            raise SystemExit(f"invalid sample for {topic}: {errors} in {record}")
    if len(records) < ROWS_PER_TOPIC:
        raise SystemExit(f"only {len(records)} samples for {topic}")
    return records[:ROWS_PER_TOPIC]


def main() -> None:
    ensure_dir(SAMPLES)
    samples: dict[str, list[dict]] = {}

    trades = synthetic_trades(rows=3_000, seed=SEED, days=5)
    for topic in TRADE_TOPICS:
        subset = [record for record in trades if INSTRUMENTS[record["symbol"]].topic == topic]
        samples[topic] = _check(subset, validate_trade, topic)

    stock_trades = pd.DataFrame(
        [record for record in trades if INSTRUMENTS[record["symbol"]].topic == "stocks.trades"]
    )
    minute_bars = aggregate_trades(stock_trades, "1min")
    bars = [
        make_bar(
            symbol=row.symbol,
            asset_class=row.asset_class,
            timeframe="1Min",
            event_time=int(row.window_start.timestamp() * 1000),
            open_=row.open,
            high=row.high,
            low=row.low,
            close=row.close,
            volume=row.volume,
            source="demo",
            vwap=None if pd.isna(row.vwap) else round(float(row.vwap), 6),
            trade_count=int(row.trade_count),
        )
        for row in minute_bars.sort_values(["window_start", "symbol"]).itertuples()
    ]
    samples["stocks.bars"] = _check(bars, validate_bar, "stocks.bars")

    samples["rates.yields"] = _check(
        synthetic_yields(days=7, seed=SEED), validate_rate, "rates.yields"
    )

    # Alert samples only demonstrate the message shape; the speed layer decides when an
    # alert really fires.
    alerts = [
        make_alert(
            symbol=row.symbol,
            alert_time=row.window_start,
            alert_type="PRICE_SPIKE" if index % 2 == 0 else "VOLUME_SPIKE",
            severity="high" if index % 5 == 0 else "medium",
            price=float(row.close),
            message=f"sample alert for {row.symbol}",
            asset_class=row.asset_class,
            zscore=3.5 + index / 10 if index % 2 == 0 else None,
            volume_zscore=None if index % 2 == 0 else 3.2 + index / 10,
        )
        for index, row in enumerate(minute_bars.head(ROWS_PER_TOPIC).itertuples())
    ]
    samples["alerts"] = alerts

    for topic, records in samples.items():
        write_jsonl(SAMPLES / f"{topic}.jsonl", records)

    counts = {topic: len(records) for topic, records in samples.items()}
    write_json(
        SAMPLES / "manifest.json",
        {
            "contract_version": CONTRACT_VERSION,
            "topics": counts,
            "format": "jsonl",
            "seed": SEED,
            "note": "Deterministic synthetic data for schema tests; not production data.",
        },
    )
    print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    main()
