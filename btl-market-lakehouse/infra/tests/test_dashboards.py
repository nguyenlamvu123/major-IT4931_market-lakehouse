"""Grafana dashboards: generated, served by market-api, metrics declared in the contract."""

from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

import pytest
import yaml
from fastapi.testclient import TestClient

from infra.scripts.build_dashboards import DASHBOARDS, render
from serving.demo_repo import DemoRepository
from serving.market_api import create_app
from serving.repository import RepositoryUnavailable

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_INFRA = yaml.safe_load((ROOT / "contracts" / "infra.yaml").read_text(encoding="utf-8"))
VARIABLE_DEFAULTS = {"${symbol}": "BTCUSDT", "${day}": ""}


class _Down:
    source = "cassandra"
    available = False

    def __getattr__(self, name: str):
        def fail(*_):
            raise RepositoryUnavailable("down")

        return fail


def _dashboard(name: str) -> dict:
    return json.loads((DASHBOARDS / name).read_text(encoding="utf-8"))


def _targets(dashboard: dict) -> list[tuple[dict, dict]]:
    return [(panel, target) for panel in dashboard["panels"] for target in panel["targets"]]


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(create_app(cassandra=_Down(), demo=DemoRepository(), demo_fallback=True))


def test_committed_dashboards_match_generator() -> None:
    for name, text in render().items():
        assert (DASHBOARDS / name).read_text(encoding="utf-8") == text, name


@pytest.mark.parametrize("name", ["market-overview.json", "infra-health.json"])
def test_panel_layout_is_valid(name: str) -> None:
    dashboard = _dashboard(name)
    ids = [panel["id"] for panel in dashboard["panels"]]
    assert len(ids) == len(set(ids))
    for panel in dashboard["panels"]:
        grid = panel["gridPos"]
        assert grid["x"] + grid["w"] <= 24, panel["title"]


def test_every_infinity_url_is_served_with_its_columns(client: TestClient) -> None:
    for panel, target in _targets(_dashboard("market-overview.json")):
        url = target["url"]
        for variable, value in VARIABLE_DEFAULTS.items():
            url = url.replace(variable, value)
        parts = urlsplit(url)
        response = client.get(parts.path, params=parse_qsl(parts.query, keep_blank_values=True))
        assert response.status_code == 200, (panel["title"], url, response.text)
        body = response.json()
        rows = body if isinstance(body, list) else [body]
        assert rows, (panel["title"], url)
        for column in target["columns"]:
            assert column["selector"] in rows[0], (panel["title"], column["selector"])


def test_market_dashboard_covers_pdf_panels() -> None:
    kinds = {panel["type"] for panel in _dashboard("market-overview.json")["panels"]}
    # PDF D.5: giá realtime, nến, cảnh báo, tương quan (+ so sánh tài sản, lợi suất).
    assert {"candlestick", "table", "timeseries", "barchart", "stat"} <= kinds
    urls = " ".join(target["url"] for _, target in _targets(_dashboard("market-overview.json")))
    for path in ("/api/v1/ohlcv", "/api/v1/alerts", "/api/v1/correlation", "/api/v1/yield-curve",
                 "/api/v1/daily/compare", "/api/v1/indicators/latest", "/api/v1/clusters"):
        assert path in urls, path


def test_monitoring_metrics_are_declared(client: TestClient) -> None:
    declared = {
        series
        for component in CONTRACT_INFRA["metrics"].values()
        for series in (component.get("series") or [])
    }
    declared |= {CONTRACT_INFRA["metrics"]["kafka_lag"]["series"]}
    api_metrics = client.get("/metrics").text
    exprs = " ".join(target["expr"] for _, target in _targets(_dashboard("infra-health.json")))
    used = {re.sub(r"_(bucket|sum|count)$", "", name) for name in re.findall(r"\bmarket_[a-z0-9_]+", exprs)}
    assert used, "no market_* metric used"
    for name in used:
        if name.startswith("market_api_"):
            assert name in api_metrics, name
        else:
            assert name in declared, name
    # PDF D.5: Kafka lag, tốc độ Spark, RAM/CPU.
    for needle in ("kafka_consumergroup_lag", "market_speed_processed_rows_per_second",
                   "container_memory_working_set_bytes", "container_cpu_usage_seconds_total"):
        assert needle in exprs, needle
