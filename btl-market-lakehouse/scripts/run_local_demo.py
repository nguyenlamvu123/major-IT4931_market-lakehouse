#!/usr/bin/env python3
"""Run the whole logical pipeline locally with pandas (no Kafka/Spark/Cassandra)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.alerts import detect_alerts  # noqa: E402
from common.analytics import (
    build_ml_features,
    correlation_matrix,
    daily_summary,
    yield_curve,
)
from common.contracts import synthetic_trades, synthetic_yields
from common.indicators import add_indicators
from common.io_utils import ensure_dir, write_json, write_jsonl
from common.ohlcv import aggregate_trades

OUT = ROOT / "data" / "demo"


def main() -> None:
    ensure_dir(OUT)

    trades = pd.DataFrame(synthetic_trades(rows=2_400, seed=42))
    bars = add_indicators(aggregate_trades(trades, "1min"))
    alerts = detect_alerts(bars)
    daily = daily_summary(bars)
    correlations = correlation_matrix(daily, window_days=90)
    rates = pd.DataFrame(synthetic_yields(days=30))
    curve = yield_curve(rates)
    features = build_ml_features(daily)

    write_jsonl(OUT / "trades.jsonl", trades.to_dict(orient="records"))
    write_jsonl(OUT / "bars.jsonl", bars.to_dict(orient="records"))
    write_json(OUT / "alerts.json", alerts)
    daily.to_csv(OUT / "daily_summary.csv", index=False)
    correlations.to_csv(OUT / "correlation_matrix.csv", index=False)
    curve.to_csv(OUT / "yield_curve.csv", index=False)
    features.to_csv(OUT / "ml_features.csv", index=False)

    summary = {
        "trades": int(len(trades)),
        "symbols": int(trades["symbol"].nunique()),
        "bars": int(len(bars)),
        "alerts": int(len(alerts)),
        "daily_rows": int(len(daily)),
        "correlation_pairs": int(len(correlations)),
        "yield_curve_rows": int(len(curve)),
        "ml_feature_rows": int(len(features)),
        "output_dir": str(OUT),
    }
    write_json(OUT / "summary.json", summary)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
