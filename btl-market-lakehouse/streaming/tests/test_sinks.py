"""Sink payloads checked against the contract (Spark local, no Kafka/Cassandra)."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from streaming.state import alert_rows

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"


def test_kafka_alert_payload_matches_alert_schema(spark) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    from common.alerts import make_alert
    from streaming.schemas import ALERT_SCHEMA
    from streaming.sinks import KafkaAlertPublisher

    alerts = alert_rows(
        [
            make_alert(
                "GC=F",
                pd.Timestamp("2026-11-09T14:31:00Z"),
                "PRICE_SPIKE",
                "high",
                2650.5,
                "GC=F price z-score 5.10",
                asset_class="gold",
                zscore=5.1,
            ),
            make_alert(
                "DXY",
                pd.Timestamp("2026-11-09T14:32:00Z"),
                "VOLUME_SPIKE",
                "medium",
                105.2,
                "DXY volume",
                asset_class="fx",
                volume_zscore=3.3,
            ),
        ]
    )
    records = KafkaAlertPublisher.to_records(spark.createDataFrame(alerts, ALERT_SCHEMA))
    rows = records.orderBy("key").collect()
    schema = json.loads((CONTRACTS / "schemas" / "alert.schema.json").read_text())
    validator = jsonschema.Draft202012Validator(schema)
    assert [row.key for row in rows] == ["DXY", "GC=F"]  # Kafka key = symbol
    for row in rows:
        value = json.loads(row.value)
        assert list(validator.iter_errors(value)) == []
    gold = json.loads(rows[1].value)
    assert gold["alert_time"] == "2026-11-09T14:31:00Z"
    assert gold["day"] == "2026-11-09"
    assert gold["alert_id"] == alerts.iloc[0]["alert_id"]
