"""Shared fixtures for speed-layer tests.

Spark tests run a real local SparkSession (no mocks) on the contract samples. They are
skipped, not failed, when the machine has no JVM or no pyspark: the pure pandas tests
still cover the logic there.
"""

from __future__ import annotations

import os
import shutil
from collections import defaultdict

import pandas as pd
import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    if not (os.environ.get("JAVA_HOME") or shutil.which("java")):
        pytest.skip("Spark tests need a JVM: set JAVA_HOME")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("speed-layer-tests")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


class MemorySink:
    """Collects every frame a sink would write, per table, as pandas."""

    def __init__(self) -> None:
        self.frames: dict[str, list[pd.DataFrame]] = defaultdict(list)

    def write(self, table: str, frame) -> None:
        from streaming.columns import check_columns

        check_columns(table, frame.columns)
        self.frames[table].append(frame.toPandas())

    def table(self, name: str) -> pd.DataFrame:
        frames = self.frames.get(name, [])
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


@pytest.fixture
def memory_sink() -> MemorySink:
    return MemorySink()
