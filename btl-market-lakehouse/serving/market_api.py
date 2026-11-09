"""FastAPI bridge between Cassandra/demo data and Grafana."""

from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, Query
from fastapi.responses import Response

from .cassandra_repo import CassandraRepository
from .demo_repo import DemoRepository

try:
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

    REQUESTS = Counter("market_api_requests_total", "Market API requests", ["path"])
except ImportError:  # pragma: no cover - optional at runtime
    CONTENT_TYPE_LATEST = "text/plain"
    REQUESTS = None

    def generate_latest() -> bytes:
        return b"# prometheus_client not installed\n"

app = FastAPI(title="BTL Market Lakehouse API", version="1.0.0")
cassandra = CassandraRepository()
demo = DemoRepository()


def repository() -> CassandraRepository | DemoRepository:
    return cassandra if cassandra.available else demo


def _count(path: str) -> None:
    if REQUESTS is not None:
        REQUESTS.labels(path=path).inc()


@app.get("/health")
def health() -> dict[str, Any]:
    _count("/health")
    return {
        "status": "ok",
        "cassandra": cassandra.available,
        "source": "cassandra" if cassandra.available else "demo",
        "data_dir": os.getenv("DEMO_DATA_DIR", "data/demo"),
    }


@app.get("/api/v1/bars")
def bars(
    symbol: str = Query("BTCUSDT"),
    limit: int = Query(60, ge=1, le=2_000),
) -> list[dict[str, Any]]:
    _count("/api/v1/bars")
    return repository().bars(symbol, limit)


@app.get("/api/v1/alerts")
def alerts(
    symbol: str = Query("BTCUSDT"),
    limit: int = Query(50, ge=1, le=500),
) -> list[dict[str, Any]]:
    _count("/api/v1/alerts")
    return repository().alerts(symbol, limit)


@app.get("/api/v1/latest")
def latest(symbol: str = Query("BTCUSDT")) -> dict[str, Any]:
    _count("/api/v1/latest")
    value = repository().latest(symbol)
    return value or {"symbol": symbol.upper(), "price": None}


@app.get("/api/v1/summary")
def summary() -> dict[str, Any]:
    _count("/api/v1/summary")
    return {
        "source": "cassandra" if cassandra.available else "demo",
        "symbols": ["BTCUSDT", "ETHUSDT", "AAPL", "GLD", "XAUUSD", "DXY"],
    }


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
