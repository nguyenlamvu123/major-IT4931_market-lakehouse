#!/usr/bin/env python3
"""Run the batch layer's maths locally with pandas (no Java, Spark or MinIO).

Call stack:

main
  -> common.contracts.synthetic_trades / synthetic_yields   90 days of the synthetic market
  -> batch.transforms.clean_trades                          silver rules (pandas mirror)
  -> common.ohlcv.aggregate_trades                          1-minute bars
  -> common.analytics.daily_summary / correlation_matrix / yield_curve / build_ml_features
  -> data/batch/{silver,gold}/                              JSONL + CSV for inspection

The cluster runs the Spark jobs in batch/jobs/; tests in batch/tests/test_gold.py prove
both produce the same numbers.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from batch.transforms import clean_trades  # noqa: E402
from common.analytics import (  # noqa: E402
    build_ml_features,
    correlation_matrix,
    daily_summary,
    yield_curve,
)
from common.contracts import synthetic_trades, synthetic_yields  # noqa: E402
from common.io_utils import df_to_records, ensure_dir, write_jsonl  # noqa: E402
from common.ohlcv import aggregate_trades  # noqa: E402

OUT = ROOT / "data" / "batch"
SEED = 42
DAYS = 90


def main() -> None:
    silver_dir = ensure_dir(OUT / "silver")
    gold_dir = ensure_dir(OUT / "gold")

    silver = clean_trades(pd.DataFrame(synthetic_trades(rows=6_000, seed=SEED, days=DAYS)))
    write_jsonl(silver_dir / "trades.jsonl", df_to_records(silver))
    rates = pd.DataFrame(synthetic_yields(days=DAYS, seed=SEED))

    daily = daily_summary(aggregate_trades(silver, "1min"))
    correlations = correlation_matrix(daily, window_days=90, rates=rates)
    curve = yield_curve(rates)
    features = build_ml_features(daily)

    daily.to_csv(gold_dir / "daily_summary.csv", index=False)
    correlations.to_csv(gold_dir / "correlation_matrix.csv", index=False)
    curve.to_csv(gold_dir / "yield_curve.csv", index=False)
    features.to_csv(gold_dir / "ml_features.csv", index=False)

    summary = {
        "silver_trades": int(len(silver)),
        "daily_rows": int(len(daily)),
        "correlation_pairs": int(len(correlations)),
        "yield_curve_rows": int(len(curve)),
        "inverted_days": int(curve["inverted"].sum()),
        "ml_feature_rows": int(len(features)),
        "output_dir": str(OUT),
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
