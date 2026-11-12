"""Demo repository returns the same columns as the Cassandra queries."""

from __future__ import annotations

from serving.demo_repo import DemoRepository
from serving.queries import (
    ALERT_COLUMNS,
    BAR_COLUMNS,
    CLUSTER_COLUMNS,
    CORRELATION_COLUMNS,
    DAILY_COLUMNS,
    INDICATOR_COLUMNS,
    YIELD_CURVE_COLUMNS,
)

REPO = DemoRepository()


def test_ohlcv_rows_have_contract_columns_newest_first() -> None:
    day = REPO.default_day()
    rows = REPO.ohlcv("SPY", day, "1m", 5)
    assert 0 < len(rows) <= 5
    assert tuple(rows[0]) == BAR_COLUMNS
    assert rows[0]["window_start"] >= rows[-1]["window_start"]
    assert all(row["day"] == day for row in rows)
    five = REPO.ohlcv("BTCUSDT", day, "5m", 50)
    assert five and tuple(five[0]) == BAR_COLUMNS


def test_snapshot_tables_have_contract_columns() -> None:
    indicators = REPO.indicators(None)
    assert len(indicators) == 12
    assert tuple(indicators[0]) == INDICATOR_COLUMNS
    assert REPO.indicators("GLD")[0]["symbol"] == "GLD"
    daily = REPO.daily("SPY", 10)
    assert len(daily) == 10
    assert tuple(daily[0]) == DAILY_COLUMNS
    correlation = REPO.correlation(90)
    assert correlation and tuple(correlation[0]) == CORRELATION_COLUMNS
    assert REPO.correlation(30) == []
    curve = REPO.yield_curve(3)
    assert len(curve) == 3
    assert tuple(curve[0]) == YIELD_CURVE_COLUMNS
    clusters = REPO.clusters()
    assert len(clusters) == 12
    assert tuple(clusters[0]) == CLUSTER_COLUMNS
    assert {row["model"] for row in clusters} == {"demo"}


def test_alerts_rows_have_contract_columns() -> None:
    days = {row["day"] for row in REPO._load().alerts.to_dict("records")}
    day = sorted(days)[-1]
    rows = REPO.alerts(day, 100)
    assert rows
    assert tuple(rows[0]) == ALERT_COLUMNS
    assert all(row["day"] == day for row in rows)
