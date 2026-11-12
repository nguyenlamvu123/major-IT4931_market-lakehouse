"""Producer Kubernetes manifests follow contracts/infra.yaml and start valid CLIs."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from common.contracts import KAFKA_BOOTSTRAP_K8S
from ingest.run_producer import SOURCES, parse_args

PROJECT = Path(__file__).resolve().parents[2]
PRODUCERS = PROJECT / "ingest" / "k8s" / "producers"


def _docs() -> list[dict]:
    docs: list[dict] = []
    for path in sorted(PRODUCERS.glob("*.yaml")):
        if path.name == "kustomization.yaml":
            continue
        docs.extend(doc for doc in yaml.safe_load_all(path.read_text()) if doc)
    return docs


def _workloads() -> list[dict]:
    return [doc for doc in _docs() if doc["kind"] in {"Deployment", "Job"}]


def test_kustomization_lists_every_manifest() -> None:
    kustomization = yaml.safe_load((PRODUCERS / "kustomization.yaml").read_text())
    files = {path.name for path in PRODUCERS.glob("*.yaml")} - {"kustomization.yaml"}
    assert set(kustomization["resources"]) == files


def test_workloads_use_contract_image_namespace_and_bootstrap() -> None:
    config = next(doc for doc in _docs() if doc["kind"] == "ConfigMap")
    assert config["data"]["KAFKA_BOOTSTRAP"] == KAFKA_BOOTSTRAP_K8S
    assert config["data"]["METRICS_PORT"] == "8001"
    sources = set()
    for doc in _workloads():
        assert doc["metadata"]["namespace"] == "market"
        container = doc["spec"]["template"]["spec"]["containers"][0]
        assert container["image"] == "btl/ingest:dev"
        assert container["imagePullPolicy"] == "IfNotPresent"
        refs = container["envFrom"]
        assert {"configMapRef": {"name": "ingest-config"}} in refs
        assert {"secretRef": {"name": "market-api-keys", "optional": True}} in refs
        assert container["ports"][0]["containerPort"] == 8001
        # Kubernetes expands $(VAR) from the container's literal env values; variables
        # coming from secrets/configMaps (or unset) are left empty here, which is fine.
        literal_env = {
            item["name"]: str(item.get("value", ""))
            for item in container.get("env", [])
            if "value" in item
        }
        args = [
            re.sub(r"\$\(([A-Z_]+)\)", lambda m, env=literal_env: env.get(m.group(1), ""), arg)
            for arg in container["args"]
        ]
        parsed = parse_args(args)
        assert parsed.source in SOURCES
        sources.add(parsed.source)
        if doc["kind"] == "Deployment":
            assert container["livenessProbe"]["httpGet"]["port"] == "metrics"
    assert sources == {"binance", "alpaca", "finnhub", "yahoo", "treasury", "fred", "replay",
                       "backfill"}


def test_live_feeds_on_by_default_and_backups_off() -> None:
    replicas = {doc["metadata"]["name"]: doc["spec"]["replicas"]
                for doc in _workloads() if doc["kind"] == "Deployment"}
    assert replicas == {
        "producer-binance": 1,
        "producer-alpaca": 1,
        "producer-yahoo": 1,
        "producer-rates-treasury": 1,
        "producer-finnhub": 0,
        "producer-rates-fred": 0,
        "producer-replay": 0,
    }


def test_service_monitor_selects_producer_service() -> None:
    service = next(doc for doc in _docs() if doc["kind"] == "Service")
    monitor = next(doc for doc in _docs() if doc["kind"] == "ServiceMonitor")
    assert monitor["metadata"]["labels"]["release"] == "kube-prometheus-stack"
    selector = monitor["spec"]["selector"]["matchLabels"]
    assert selector.items() <= service["metadata"]["labels"].items()
    assert service["spec"]["selector"].items() <= _workloads()[0]["spec"]["template"][
        "metadata"]["labels"].items()
    assert monitor["spec"]["endpoints"][0]["port"] == service["spec"]["ports"][0]["name"]
