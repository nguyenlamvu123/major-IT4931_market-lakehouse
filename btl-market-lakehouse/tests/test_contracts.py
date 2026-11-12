"""Keep contracts/ (reviewed files) and common/contracts.py (executable mirror) in sync."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from common.analytics import CORRELATION_COLUMNS, YIELD_CURVE_COLUMNS
from common.contracts import (
    ASSET_CLASSES,
    INSTRUMENTS,
    KAFKA_BOOTSTRAP_K8S,
    RATE_SERIES,
    TOPICS,
    validate_bar,
    validate_rate,
    validate_trade,
)
from common.cql import load_tables

CONTRACTS = Path(__file__).resolve().parents[1] / "contracts"
SCHEMA_FOR = {
    "trade": "trade.schema.json",
    "bar": "bar.schema.json",
    "rate_yield": "rate_yield.schema.json",
    "alert": "alert.schema.json",
}
PYTHON_VALIDATOR = {"trade": validate_trade, "bar": validate_bar, "rate_yield": validate_rate}


def _topics_yaml() -> dict:
    return yaml.safe_load((CONTRACTS / "topics.yaml").read_text(encoding="utf-8"))


def test_topics_yaml_matches_python_mirror() -> None:
    document = _topics_yaml()
    assert document["cluster"]["bootstrap_k8s"] == KAFKA_BOOTSTRAP_K8S
    declared = {topic["name"]: topic for topic in document["topics"]}
    assert set(declared) == set(TOPICS)
    for name, spec in TOPICS.items():
        assert declared[name]["partitions"] == spec["partitions"], name
        assert declared[name]["retention_ms"] == spec["retention_ms"], name
        assert declared[name]["key"] == spec["key"], name
        assert declared[name]["producer"] == spec["producer"], name


def test_topics_yaml_symbols_match_registry() -> None:
    for topic in _topics_yaml()["topics"]:
        if topic["name"] == "stocks.bars":
            continue  # bars reuse the stocks.trades symbol list
        for symbol in topic.get("symbols", []):
            assert INSTRUMENTS[symbol].topic == topic["name"], symbol


def test_rate_schema_enums_match_registry() -> None:
    schema = json.loads((CONTRACTS / "schemas" / "rate_yield.schema.json").read_text())
    assert schema["properties"]["series_id"]["enum"] == list(RATE_SERIES)
    assert schema["properties"]["tenor"]["enum"] == list(RATE_SERIES.values())
    trade = json.loads((CONTRACTS / "schemas" / "trade.schema.json").read_text())
    assert set(trade["properties"]["asset_class"]["enum"]) == set(ASSET_CLASSES) - {"rates"}


@pytest.mark.parametrize("topic", [name for name in TOPICS if name != "dead-letter"])
def test_samples_have_20_valid_rows(topic: str) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    path = CONTRACTS / "samples" / f"{topic}.jsonl"
    assert path.exists(), "run: python scripts/generate_samples.py"
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
    assert len(rows) == 20

    kind = TOPICS[topic]["schema"]
    schema = json.loads((CONTRACTS / "schemas" / SCHEMA_FOR[kind]).read_text())
    validator = jsonschema.Draft202012Validator(schema)
    key_field = TOPICS[topic]["key"]
    for row in rows:
        assert list(validator.iter_errors(row)) == []
        assert row.get(key_field), f"missing Kafka key field {key_field}"
        if kind in PYTHON_VALIDATOR:
            assert PYTHON_VALIDATOR[kind](row) == []
        if kind == "trade":
            assert INSTRUMENTS[row["symbol"]].topic == topic


def test_cassandra_contract_tables_and_keys() -> None:
    tables = load_tables()
    assert set(tables) == {
        "ohlcv_1m",
        "ohlcv_5m",
        "indicators_latest",
        "alerts_by_day",
        "daily_summary",
        "correlation_matrix",
        "yield_curve",
        "asset_clusters",
    }
    assert tables["ohlcv_1m"].partition_key == ("symbol", "day")
    assert tables["ohlcv_1m"].clustering_key == ("window_start",)
    assert tables["ohlcv_5m"].columns == tables["ohlcv_1m"].columns
    assert tables["indicators_latest"].partition_key == ("symbol",)
    assert tables["alerts_by_day"].partition_key == ("day",)
    # The pandas reference implementations produce exactly the batch table columns.
    assert set(tables["correlation_matrix"].columns) == set(CORRELATION_COLUMNS)
    assert set(tables["yield_curve"].columns) == set(YIELD_CURVE_COLUMNS) | {"curve"}


def test_alert_sample_columns_fit_alerts_by_day() -> None:
    tables = load_tables()
    path = CONTRACTS / "samples" / "alerts.jsonl"
    row = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    assert set(row) <= set(tables["alerts_by_day"].columns)


def test_backfill_symbols_cover_registry() -> None:
    bars = next(topic for topic in _topics_yaml()["topics"] if topic["name"] == "stocks.bars")
    assert set(bars["backfill_symbols"]) == set(INSTRUMENTS)
