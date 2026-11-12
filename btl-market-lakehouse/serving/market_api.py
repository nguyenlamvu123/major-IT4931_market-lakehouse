"""market-api: read-only JSON bridge from Cassandra (keyspace ``market``) to Grafana.

Grafana has no first-party Cassandra datasource, so the market dashboard reads these
endpoints through the Infinity plugin (PDF D.5: giá realtime, nến, cảnh báo, tương quan).

Data-source policy:

- Cassandra answers whenever it is reachable, even with an empty result ("no data yet").
- If Cassandra is down: with ``DEMO_FALLBACK=true`` (laptop default) the synthetic
  ``DemoRepository`` answers; with ``DEMO_FALLBACK=false`` (Kubernetes manifest) the API
  returns HTTP 503, so a dashboard during the evaluation never shows fake numbers.
- Every response carries ``X-Data-Source: cassandra|demo|none``.

Call stack of a data request:

    GET /api/v1/... -> {@link _answer}
      -> CassandraRepository.<method>   (RepositoryUnavailable on failure)
      -> DemoRepository.<method>         (only if DEMO_FALLBACK)
      -> serving.views (ordering / reshaping)
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    ProcessCollector,
    generate_latest,
)

from common.contracts import INSTRUMENTS

from . import views
from .cassandra_repo import CassandraRepository
from .demo_repo import DemoRepository
from .repository import MarketRepository, RepositoryUnavailable

DAY_PATTERN = r"^\d{4}-\d{2}-\d{2}$"
SOURCE_HEADER = "X-Data-Source"


def _env_flag(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def _symbol(value: str) -> str:
    symbol = value.strip().upper()
    if symbol not in INSTRUMENTS:
        raise HTTPException(status_code=404, detail=f"unknown symbol {value!r}")
    return symbol


def create_app(
    cassandra: MarketRepository | None = None,
    demo: MarketRepository | None = None,
    demo_fallback: bool | None = None,
) -> FastAPI:
    """Build the API; tests inject repositories, uvicorn uses the module-level ``app``."""
    cassandra = cassandra or CassandraRepository()
    demo = demo or DemoRepository()
    fallback = _env_flag("DEMO_FALLBACK", True) if demo_fallback is None else demo_fallback

    # A private registry: create_app() may run several times in one process (tests), and
    # the default global registry rejects duplicate metric names.
    registry = CollectorRegistry()
    ProcessCollector(registry=registry)
    requests_total = Counter(
        "market_api_requests_total",
        "market-api requests",
        ["path", "source", "status"],
        registry=registry,
    )
    latency = Histogram(
        "market_api_request_duration_seconds",
        "market-api request latency",
        ["path"],
        registry=registry,
    )

    app = FastAPI(title="BTL Market Lakehouse API", version="2.0.0")

    @app.middleware("http")
    async def label_and_measure(request: Request, call_next: Callable) -> Response:
        started = time.perf_counter()
        request.state.data_source = "none"
        response = await call_next(request)
        route = request.scope.get("route")
        # Label by route template, not raw URL, to keep metric cardinality bounded.
        path = getattr(route, "path", "unmatched")
        source = request.state.data_source
        response.headers[SOURCE_HEADER] = source
        if path != "/metrics":
            latency.labels(path=path).observe(time.perf_counter() - started)
            requests_total.labels(path=path, source=source, status=str(response.status_code)).inc()
        return response

    def _answer(request: Request, call: Callable[[MarketRepository], Any]) -> Any:
        try:
            result = call(cassandra)
            request.state.data_source = cassandra.source
            return result
        except RepositoryUnavailable as exc:
            if not fallback:
                raise HTTPException(status_code=503, detail=str(exc)) from exc
        result = call(demo)
        request.state.data_source = demo.source
        return result

    @app.get("/health")
    def health(request: Request) -> dict[str, Any]:
        # Liveness/readiness of the API process itself: always 200, so Kubernetes does not
        # restart the API just because Cassandra is still starting.
        cassandra_up = getattr(cassandra, "available", True)
        if cassandra_up:
            source = cassandra.source
        elif fallback:
            source = demo.source
        else:
            source = "none"
        request.state.data_source = source
        return {
            "status": "ok",
            "cassandra": cassandra_up,
            "demo_fallback": fallback,
            "source": source,
        }

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    @app.get("/api/v1/symbols")
    def symbols() -> list[dict[str, Any]]:
        return [
            {"symbol": symbol, "asset_class": item.asset_class, "topic": item.topic}
            for symbol, item in INSTRUMENTS.items()
        ]

    @app.get("/api/v1/ohlcv")
    def ohlcv(
        request: Request,
        symbol: str = Query("BTCUSDT"),
        day: str | None = Query(None, pattern=DAY_PATTERN),
        interval: str = Query("1m", pattern="^(1m|5m)$"),
        limit: int = Query(390, ge=1, le=1_440),
    ) -> list[dict[str, Any]]:
        symbol = _symbol(symbol)
        rows = _answer(
            request, lambda repo: repo.ohlcv(symbol, day or repo.default_day(), interval, limit)
        )
        return views.ascending(rows, "window_start")

    @app.get("/api/v1/indicators/latest")
    def indicators(request: Request, symbol: str | None = Query(None)) -> list[dict[str, Any]]:
        chosen = _symbol(symbol) if symbol else None
        return _answer(request, lambda repo: repo.indicators(chosen))

    @app.get("/api/v1/alerts")
    def alerts(
        request: Request,
        day: str | None = Query(None, pattern=DAY_PATTERN),
        limit: int = Query(200, ge=1, le=2_000),
    ) -> list[dict[str, Any]]:
        return _answer(request, lambda repo: repo.alerts(day or repo.default_day(), limit))

    @app.get("/api/v1/daily")
    def daily(
        request: Request,
        symbol: str = Query("SPY"),
        limit: int = Query(90, ge=1, le=3_650),
    ) -> list[dict[str, Any]]:
        symbol = _symbol(symbol)
        return views.ascending(_answer(request, lambda repo: repo.daily(symbol, limit)), "day")

    @app.get("/api/v1/daily/compare")
    def daily_compare(
        request: Request,
        symbols: str = Query("SPY,GLD,TLT,BTCUSDT"),
        limit: int = Query(90, ge=2, le=3_650),
    ) -> list[dict[str, Any]]:
        chosen = [_symbol(item) for item in symbols.split(",") if item.strip()]
        series = _answer(
            request, lambda repo: {symbol: repo.daily(symbol, limit) for symbol in chosen}
        )
        return views.normalized_compare(series)

    @app.get("/api/v1/correlation")
    def correlation(
        request: Request,
        window_days: int = Query(90, ge=2, le=1_000),
        format: str = Query("pairs", pattern="^(pairs|matrix)$"),
    ) -> list[dict[str, Any]]:
        rows = _answer(request, lambda repo: repo.correlation(window_days))
        return views.correlation_matrix(rows) if format == "matrix" else rows

    @app.get("/api/v1/yield-curve")
    def yield_curve(
        request: Request,
        limit: int = Query(90, ge=1, le=3_650),
    ) -> list[dict[str, Any]]:
        return views.ascending(_answer(request, lambda repo: repo.yield_curve(limit)), "day")

    @app.get("/api/v1/yield-curve/latest")
    def yield_curve_latest(request: Request) -> list[dict[str, Any]]:
        rows = _answer(request, lambda repo: repo.yield_curve(1))
        return views.curve_points(rows[0] if rows else None)

    @app.get("/api/v1/clusters")
    def clusters(request: Request) -> list[dict[str, Any]]:
        return _answer(request, lambda repo: repo.clusters())

    return app


app = create_app()
