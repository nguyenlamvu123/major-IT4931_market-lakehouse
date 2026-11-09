#!/usr/bin/env python3
"""Generate deterministic JSONL samples under contracts/samples."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.contracts import (  # noqa: E402
    ASSET_CLASSES,
    synthetic_trades,
    synthetic_yields,
    topic_for,
    validate_trade,
)
from common.io_utils import ensure_dir, write_json, write_jsonl  # noqa: E402

SAMPLES = ROOT / "contracts" / "samples"


def main() -> None:
    ensure_dir(SAMPLES)
    trades = [record for record in synthetic_trades(rows=240, seed=11) if not validate_trade(record)]
    counts: dict[str, int] = {}

    for asset_class in ASSET_CLASSES:
        if asset_class == "rates":
            continue
        topic = topic_for(asset_class, "trades")
        subset = [record for record in trades if record["asset_class"] == asset_class]
        write_jsonl(SAMPLES / f"{topic}.jsonl", subset)
        counts[topic] = len(subset)

    rates = synthetic_yields(days=3, seed=11)
    write_jsonl(SAMPLES / "rates.ticks.jsonl", rates)
    counts["rates.ticks"] = len(rates)

    write_json(
        SAMPLES / "manifest.json",
        {
            "topics": counts,
            "format": "jsonl",
            "note": "Deterministic sample data for schema tests; not production data.",
        },
    )
    print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    main()
