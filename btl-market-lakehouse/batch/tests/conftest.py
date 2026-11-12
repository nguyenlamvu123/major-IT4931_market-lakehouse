"""Shared fixtures for batch tests: a small local SparkSession and a fake bronze lake."""

from __future__ import annotations

import json
import os
import shutil
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest


def _java_available() -> bool:
    return bool(shutil.which("java") or os.environ.get("JAVA_HOME"))


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession for the whole test run.

    Spark needs a JVM; CI images without Java (and environments without pyspark) skip
    the Spark tests instead of failing, while the pure-Python tests still run.
    """
    pytest.importorskip("pyspark")
    if not _java_available():
        pytest.skip("Java is not installed; Spark tests need a JVM")
    from batch.spark_session import build_spark

    session = build_spark(
        "batch-tests",
        master="local[2]",
        shuffle_partitions=2,
        extra_conf={
            "spark.driver.memory": "1g",
            "spark.ui.enabled": "false",
            "spark.sql.adaptive.enabled": "false",
        },
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _record_day(topic: str, record: dict[str, Any]) -> str:
    """UTC date the Kafka Connect TimeBasedPartitioner would write the record under."""
    if topic == "alerts":
        return str(record["alert_time"])[:10]
    millis = int(record["event_time"])
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).date().isoformat()


def write_bronze(
    lake_dir: Path,
    topic: str,
    records: Iterable[dict[str, Any]],
    extra_lines: dict[str, list[str]] | None = None,
) -> None:
    """Write records the way the S3 sink lays them out: bronze/<topic>/dt=<day>/*.json.

    ``extra_lines`` maps a day to raw lines appended to that day's file (used to inject
    malformed JSON).
    """
    by_day: dict[str, list[str]] = defaultdict(list)
    for record in records:
        by_day[_record_day(topic, record)].append(json.dumps(record))
    for day, lines in (extra_lines or {}).items():
        by_day[day].extend(lines)
    for day, lines in by_day.items():
        target = lake_dir / "bronze" / topic / f"dt={day}"
        target.mkdir(parents=True, exist_ok=True)
        (target / f"{topic}+0+0000000000.json").write_text("\n".join(lines) + "\n")


@pytest.fixture()
def lake_dir(tmp_path: Path) -> Path:
    root = tmp_path / "lake"
    root.mkdir()
    return root


@pytest.fixture()
def bronze_writer():
    """Fixture form of :func:`write_bronze` (conftest modules are not importable)."""
    return write_bronze
