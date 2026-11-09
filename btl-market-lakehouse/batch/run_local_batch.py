#!/usr/bin/env python3
"""Run the medallion transforms locally with pandas (no Spark/MinIO)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from batch.transforms import (  # noqa: E402
    build_correlations,
    build_curve,
    build_features,
    clean_trades,
    daily_bars,
)
from common.contracts import synthetic_trades, synthetic_yields  # noqa: E402
from common.io_utils import df_to_records, ensure_dir, write_jsonl  # noqa: E402
from common.ohlcv import aggregate_trades  # noqa: E402

OUT = ROOT / "data" / "batch"


def main() -> None:
    silver_dir = ensure_dir(OUT / "silver")
    gold_dir = ensure_dir(OUT / "gold")

    trades = pd.DataFrame(synthetic_trades(rows=2_400, seed=42))
    trades = trades[trades["asset_class"] != "rates"].reset_index(drop=True)
    silver = clean_trades(trades)
    write_jsonl(silver_dir / "trades.jsonl", df_to_records(silver))

    bars = aggregate_trades(silver, "1min")
    daily = daily_bars(bars)
    daily.to_csv(gold_dir / "daily_bars.csv", index=False)

    correlations = build_correlations(daily, window_days=90)
    correlations.to_csv(gold_dir / "correlation_matrix.csv", index=False)

    features = build_features(daily)
    features.to_csv(gold_dir / "ml_features.csv", index=False)

    rates = pd.DataFrame(synthetic_yields(days=30, seed=7))
    curve = build_curve(rates)
    curve.to_csv(gold_dir / "yield_curve.csv", index=False)

    summary = {
        "silver_trades": int(len(silver)),
        "daily_rows": int(len(daily)),
        "correlation_pairs": int(len(correlations)),
        "ml_feature_rows": int(len(features)),
        "yield_curve_rows": int(len(curve)),
        "output_dir": str(OUT),
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
