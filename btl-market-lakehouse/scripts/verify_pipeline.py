#!/usr/bin/env python3
"""Check that the local demo output exists and looks sane."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "demo"

REQUIRED = [
    "trades.jsonl",
    "bars.jsonl",
    "alerts.json",
    "daily_summary.csv",
    "correlation_matrix.csv",
    "yield_curve.csv",
    "ml_features.csv",
    "summary.json",
]


def main() -> None:
    missing = [name for name in REQUIRED if not (OUT / name).exists()]
    if missing:
        raise SystemExit(
            f"Missing demo output: {missing}. Run: python scripts/run_local_demo.py"
        )

    summary = json.loads((OUT / "summary.json").read_text(encoding="utf-8"))
    assert summary["trades"] > 0
    assert summary["bars"] > 0
    assert summary["symbols"] > 0
    print("OK", json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
