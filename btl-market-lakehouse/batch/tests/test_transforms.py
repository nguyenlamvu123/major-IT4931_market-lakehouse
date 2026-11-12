"""Pandas mirror of the silver trade rules (used by run_local_batch.py)."""

from __future__ import annotations

import pandas as pd

from batch.transforms import clean_trades
from common.contracts import make_trade

EVENT_TIME = 1_788_273_000_000  # 2026-09-01T14:30:00Z


def test_clean_trades_deduplicates_and_filters() -> None:
    first = make_trade("AAPL", "equity", EVENT_TIME, 230.0, 2.0, "alpaca", exchange_trade_id=1)
    trades = pd.DataFrame(
        [
            first,
            dict(first, ingest_time=2),  # re-sent copy
            make_trade("AAPL", "equity", EVENT_TIME + 1, -1.0, 1.0, "alpaca", exchange_trade_id=2),
            make_trade("US10Y", "rates", EVENT_TIME + 2, 4.1, 0.0, "alpaca", exchange_trade_id=3),
            make_trade("bad sym", "equity", EVENT_TIME + 3, 1.0, 1.0, "alpaca", exchange_trade_id=4),
        ]
    )
    result = clean_trades(trades)
    assert list(result["trade_id"]) == [first["trade_id"]]
    assert result.iloc[0]["dt"] == "2026-09-01"
    assert result.iloc[0]["event_time_ms"] == EVENT_TIME
