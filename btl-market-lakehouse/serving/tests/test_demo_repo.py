"""Demo repository tests (no Cassandra, no FastAPI server)."""

from __future__ import annotations

from serving.demo_repo import DemoRepository


def test_demo_repository_bars_and_latest() -> None:
    repo = DemoRepository()
    bars = repo.bars("BTCUSDT", 10)
    assert 0 < len(bars) <= 10
    assert "close" in bars[0]

    latest = repo.latest("BTCUSDT")
    assert latest is not None
    assert latest["symbol"] == "BTCUSDT"


def test_demo_repository_alerts_shape() -> None:
    repo = DemoRepository()
    alerts = repo.alerts("BTCUSDT", 5)
    assert isinstance(alerts, list)
