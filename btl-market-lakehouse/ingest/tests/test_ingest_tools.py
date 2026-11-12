"""Measurement tools used for the role A report."""

from __future__ import annotations

import pytest

from common.contracts import make_trade
from ingest.tools.bench_publish import bench
from ingest.tools.feed_quality import feed_quality
from ingest.tools.topic_rates import markdown_row, rates_between


def test_rates_between_uses_watermark_deltas() -> None:
    rates = rates_between({"crypto.trades": 1_000, "alerts": 5},
                          {"crypto.trades": 4_000, "alerts": 5, "fx.dxy": 60}, 60.0)
    assert rates == {"alerts": 0.0, "crypto.trades": 50.0, "fx.dxy": 1.0}
    # A recreated topic restarts at offset 0: report 0, not a negative rate.
    assert rates_between({"x": 10}, {"x": 3}, 1.0) == {"x": 0.0}
    with pytest.raises(ValueError):
        rates_between({}, {}, 0)
    assert "crypto.trades=50.00" in markdown_row(rates, 60.0)


def test_feed_quality_counts_duplicates_late_and_out_of_order() -> None:
    def trade(event_time: int, exchange_id: int, ingest_delay: int) -> dict:
        return make_trade("BTCUSDT", "crypto", event_time, 92_400.0, 0.1, "binance",
                          exchange_trade_id=exchange_id, ingest_time=event_time + ingest_delay)

    records = [
        trade(1_000, 1, 100),
        trade(2_000, 2, 200),
        trade(2_000, 2, 900),     # re-sent after reconnect
        trade(1_500, 3, 45_000),  # arrived after a later trade and 45 s late
    ]
    summary = feed_quality(records, late_after_ms=30_000)
    assert summary["records"] == 4
    assert summary["duplicate_pct"] == 25.0
    assert summary["out_of_order_pct"] == 25.0
    assert summary["late_pct"] == 25.0
    assert summary["ingest_lag_p50_ms"] == 200.0
    assert summary["ingest_lag_max_ms"] == 45_000.0


def test_bench_publish_sends_every_valid_record() -> None:
    result = bench(rows=500)
    assert result["sent"] == result["records"] > 0
    assert result["records_per_second"] > 0
