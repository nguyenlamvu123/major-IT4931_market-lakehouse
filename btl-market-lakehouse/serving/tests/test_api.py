"""HTTP behaviour of market-api, with injected repositories (no Cassandra, no network)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from serving.cassandra_repo import CassandraRepository, _plain
from serving.market_api import SOURCE_HEADER, create_app
from serving.repository import RepositoryUnavailable


class DownRepository:
    """Cassandra that is not reachable."""

    source = "cassandra"
    available = False

    def __getattr__(self, name: str) -> Any:
        def fail(*_: Any) -> Any:
            raise RepositoryUnavailable("down")

        return fail


class EmptyRepository:
    """Cassandra that is up but has no rows yet (fresh cluster)."""

    source = "cassandra"
    available = True

    def default_day(self) -> str:
        return "2026-11-12"

    def __getattr__(self, name: str) -> Any:
        return lambda *_: []


class FakeDemo:
    source = "demo"

    def default_day(self) -> str:
        return "2026-11-09"

    def ohlcv(self, symbol: str, day: str, interval: str, limit: int) -> list[dict]:
        return [
            {"symbol": symbol, "day": day, "window_start": "2026-11-09T10:01:00+00:00"},
            {"symbol": symbol, "day": day, "window_start": "2026-11-09T10:00:00+00:00"},
        ]

    def daily(self, symbol: str, limit: int) -> list[dict]:
        return [{"symbol": symbol, "day": "2026-11-03", "close": 110.0},
                {"symbol": symbol, "day": "2026-11-02", "close": 100.0}]

    def correlation(self, window_days: int) -> list[dict]:
        return [{"base_symbol": "GLD", "quote_symbol": "US10Y", "correlation": -0.5,
                 "window_days": window_days, "as_of": "2026-11-09", "n_obs": 60}]

    def yield_curve(self, limit: int) -> list[dict]:
        return [{"day": "2026-11-09", "y_2y": 4.0, "y_10y": 4.2}][:limit]


def _client(cassandra: Any, fallback: bool) -> TestClient:
    return TestClient(create_app(cassandra=cassandra, demo=FakeDemo(), demo_fallback=fallback))


def test_cassandra_down_with_fallback_serves_labelled_demo_data() -> None:
    response = _client(DownRepository(), fallback=True).get("/api/v1/ohlcv?symbol=btcusdt")
    assert response.status_code == 200
    assert response.headers[SOURCE_HEADER] == "demo"
    rows = response.json()
    # Repositories return newest first; the API serves charts in ascending time.
    assert [row["window_start"] for row in rows] == sorted(row["window_start"] for row in rows)
    assert rows[0]["day"] == "2026-11-09"


def test_cassandra_down_without_fallback_returns_503_not_fake_data() -> None:
    # Regression: the old API silently switched to synthetic data, so a dashboard on the
    # cluster could show invented prices during the evaluation.
    response = _client(DownRepository(), fallback=False).get("/api/v1/ohlcv?symbol=BTCUSDT")
    assert response.status_code == 503
    assert response.headers[SOURCE_HEADER] == "none"


def test_empty_cassandra_is_not_replaced_by_demo_data() -> None:
    response = _client(EmptyRepository(), fallback=True).get("/api/v1/alerts")
    assert response.status_code == 200
    assert response.json() == []
    assert response.headers[SOURCE_HEADER] == "cassandra"


def test_health_reports_source_and_policy() -> None:
    body = _client(DownRepository(), fallback=False).get("/health").json()
    assert body == {"status": "ok", "cassandra": False, "demo_fallback": False, "source": "none"}
    body = _client(DownRepository(), fallback=True).get("/health").json()
    assert body["source"] == "demo"


def test_unknown_symbol_is_404() -> None:
    response = _client(EmptyRepository(), fallback=True).get("/api/v1/daily?symbol=NOPE")
    assert response.status_code == 404


def test_compare_correlation_and_curve_views() -> None:
    client = _client(DownRepository(), fallback=True)
    compare = client.get("/api/v1/daily/compare?symbols=SPY,GLD").json()
    assert compare[-1] == {"day": "2026-11-03", "SPY": 110.0, "GLD": 110.0}
    matrix = client.get("/api/v1/correlation?format=matrix").json()
    assert matrix[0] == {"symbol": "GLD", "GLD": 1.0, "US10Y": -0.5}
    pairs = client.get("/api/v1/correlation").json()
    assert pairs[0]["n_obs"] == 60
    points = client.get("/api/v1/yield-curve/latest").json()
    assert [point["tenor"] for point in points] == ["2Y", "10Y"]


def test_symbols_come_from_contract_registry() -> None:
    rows = _client(EmptyRepository(), fallback=True).get("/api/v1/symbols").json()
    by_symbol = {row["symbol"]: row for row in rows}
    assert by_symbol["GLD"] == {"symbol": "GLD", "asset_class": "gold", "topic": "stocks.trades"}
    assert len(rows) == 12


def test_metrics_expose_latency_histogram_by_route() -> None:
    client = _client(DownRepository(), fallback=True)
    client.get("/api/v1/ohlcv?symbol=SPY")
    text = client.get("/metrics").text
    assert 'market_api_request_duration_seconds_bucket{le="0.005",path="/api/v1/ohlcv"}' in text
    assert 'market_api_requests_total{path="/api/v1/ohlcv",source="demo",status="200"}' in text
    assert "process_resident_memory_bytes" in text


class FakeSession:
    def __init__(self, results: dict[str, list[dict]], fail: bool = False) -> None:
        self.results = results
        self.fail = fail
        self.calls: list[tuple[str, tuple]] = []

    def execute(self, cql: str, params: tuple) -> list[dict]:
        self.calls.append((cql, params))
        if self.fail:
            raise RuntimeError("connection reset")
        table = cql.split(" FROM market.")[1].split(" ")[0]
        key = "as_of" if cql.startswith("SELECT as_of ") else table
        return self.results.get(key, [])


def test_correlation_reads_latest_as_of_partition_slice() -> None:
    session = FakeSession(
        {
            "as_of": [{"as_of": "2026-11-09"}],
            "correlation_matrix": [{"base_symbol": "GLD", "quote_symbol": "US10Y"}],
        }
    )
    rows = CassandraRepository(session=session).correlation(90)
    assert rows == [{"base_symbol": "GLD", "quote_symbol": "US10Y"}]
    assert session.calls[0][1] == (90, 1)
    assert session.calls[1][1] == (90, "2026-11-09")


def test_query_failure_raises_instead_of_returning_empty_list() -> None:
    # Regression: a failed query used to return [] and the dashboard showed "no data".
    with pytest.raises(RepositoryUnavailable):
        CassandraRepository(session=FakeSession({}, fail=True)).alerts("2026-11-09", 10)


def test_driver_values_become_utc_json() -> None:
    assert _plain(datetime(2026, 11, 9, 10, 0)) == "2026-11-09T10:00:00+00:00"
    assert _plain(4.2) == 4.2
    assert _plain(None) is None

    class DriverDate:
        def __str__(self) -> str:
            return "2026-11-09"

    assert _plain(DriverDate()) == "2026-11-09"
