#!/usr/bin/env python3
"""Run the whole logical pipeline locally with pandas (no Kafka/Spark/Cassandra).

Mirrors the Lambda flow of the PDF: trades -> 1m bars + indicators + alerts (speed
layer), then daily summary, cross-asset correlation with yields and the yield curve
(batch layer). Output goes to data/demo/ and is checked by verify_pipeline.py.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.alerts import detect_alerts  # noqa: E402
from common.analytics import (  # noqa: E402
    build_ml_features,
    correlation_matrix,
    daily_summary,
    yield_curve,
)
from common.contracts import synthetic_trades, synthetic_yields  # noqa: E402
from common.indicators import add_indicators  # noqa: E402
from common.io_utils import ensure_dir, write_json, write_jsonl  # noqa: E402
from common.ohlcv import aggregate_trades  # noqa: E402

OUT = ROOT / "data" / "demo"
SEED = 42
DAYS = 90


def main() -> None:
    ensure_dir(OUT)

    trades = pd.DataFrame(synthetic_trades(rows=6_000, seed=SEED, days=DAYS))
    bars = add_indicators(aggregate_trades(trades, "1min"))
    alerts = detect_alerts(bars)
    daily = daily_summary(bars)
    rates = pd.DataFrame(synthetic_yields(days=DAYS, seed=SEED))
    correlations = correlation_matrix(daily, window_days=90, rates=rates)
    curve = yield_curve(rates)
    features = build_ml_features(daily)

    write_jsonl(OUT / "trades.jsonl", trades.to_dict(orient="records"))
    write_jsonl(OUT / "bars.jsonl", bars.to_dict(orient="records"))
    write_json(OUT / "alerts.json", alerts)
    daily.to_csv(OUT / "daily_summary.csv", index=False)
    correlations.to_csv(OUT / "correlation_matrix.csv", index=False)
    curve.to_csv(OUT / "yield_curve.csv", index=False)
    features.to_csv(OUT / "ml_features.csv", index=False)

    def corr(base: str, quote: str) -> float | None:
        pair = sorted((base, quote))
        match = correlations[
            (correlations["base_symbol"] == pair[0]) & (correlations["quote_symbol"] == pair[1])
        ]
        return None if match.empty else round(float(match.iloc[0]["correlation"]), 3)

    summary = {
        "trades": int(len(trades)),
        "symbols": int(trades["symbol"].nunique()),
        "bars": int(len(bars)),
        "alerts": int(len(alerts)),
        "daily_rows": int(len(daily)),
        "correlation_pairs": int(len(correlations)),
        "yield_curve_rows": int(len(curve)),
        "inverted_days": int(curve["inverted"].sum()),
        "ml_feature_rows": int(len(features)),
        # The three questions from the "So sánh với trái phiếu" slide, answered on the
        # synthetic market (numbers are only meaningful on real data).
        "corr_SPY_TLT": corr("SPY", "TLT"),
        "corr_TLT_US10Y": corr("TLT", "US10Y"),
        "corr_GLD_REAL10Y": corr("GLD", "REAL10Y"),
        "corr_BTCUSDT_GC=F": corr("BTCUSDT", "GC=F"),
        "output_dir": str(OUT),
    }
    write_json(OUT / "summary.json", summary)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
