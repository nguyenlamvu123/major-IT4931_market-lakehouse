"""Fault-injection scripts parse and are documented where results are recorded."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

INFRA = Path(__file__).resolve().parents[1]
FAULTS = INFRA / "fault_injection"
LESSONS = INFRA.parent / "docs" / "lessons" / "D-fault-injection.md"
SCRIPTS = sorted(FAULTS.rglob("*.sh"))


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda path: str(path.relative_to(FAULTS)))
def test_fault_scripts_parse(script: Path) -> None:
    result = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_every_k8s_scenario_is_in_the_results_log() -> None:
    log = LESSONS.read_text(encoding="utf-8")
    for script in (FAULTS / "k8s").glob("*.sh"):
        if script.name in {"lib.sh", "restore.sh"}:
            continue
        assert f"k8s/{script.name}" in log, script.name


def test_k8s_scripts_target_contract_resources() -> None:
    text = "\n".join(path.read_text(encoding="utf-8") for path in (FAULTS / "k8s").glob("*.sh"))
    # Labels set by Strimzi (cluster "market"), cass-operator (cluster "market") and the
    # spark-operator; namespaces from contracts/infra.yaml.
    assert "strimzi.io/cluster=market" in text
    assert "cassandra.datastax.com/cluster=market" in text
    assert "sparkoperator.k8s.io/app-name=" in text
    for namespace in ("-n kafka", "-n spark", "-n cassandra", "-n minio", "-n market"):
        assert namespace in text, namespace
