"""Checks for the make up/down/images/secrets scripts (no cluster needed)."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
import yaml

INFRA = Path(__file__).resolve().parents[1]
ROOT = INFRA.parent
SCRIPTS = INFRA / "scripts"
CONTRACT_INFRA = yaml.safe_load((ROOT / "contracts" / "infra.yaml").read_text(encoding="utf-8"))


@pytest.mark.parametrize("script", sorted(SCRIPTS.glob("*.sh")), ids=lambda path: path.name)
def test_shell_scripts_parse(script: Path) -> None:
    result = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_makefile_exposes_pdf_targets() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    targets = set(re.findall(r"^([a-z0-9-]+):", makefile, flags=re.MULTILINE))
    # PDF D.3 (make up / make down) plus the helpers the README promises.
    assert {"up", "down", "images", "secrets", "status", "port-forward"} <= targets
    # Laptop workflow from before must keep working.
    assert {"dev-up", "dev-down", "topics", "cassandra-init", "demo", "test"} <= targets


def test_up_installs_every_helm_values_file_with_pinned_versions() -> None:
    up = (SCRIPTS / "up.sh").read_text(encoding="utf-8")
    versions = (INFRA / "k8s" / "helm" / "versions.env").read_text(encoding="utf-8")
    declared = set(re.findall(r"^([A-Z_]+)=", versions, flags=re.MULTILINE))
    used = set(re.findall(r"\$\{([A-Z_]+_CHART_VERSION)\}", up))
    assert used == declared
    for values in (INFRA / "k8s" / "helm").glob("*-values.yaml"):
        assert values.name in up, values.name


def test_up_order_respects_dependencies() -> None:
    up = (SCRIPTS / "up.sh").read_text(encoding="utf-8")

    def position(marker: str) -> int:
        index = up.find(marker)
        assert index >= 0, marker
        return index

    # cert-manager before k8ssandra (webhook certificates); Prometheus CRDs before the
    # ServiceMonitor/PodMonitor objects in the platform manifests; secrets before pods.
    assert position("secrets.sh") < position("release cert-manager")
    assert position("release cert-manager") < position("release k8ssandra-operator")
    assert position("release kube-prometheus-stack") < position("render_platform |")
    assert position("render_platform |") < position("release airflow")


def test_images_script_builds_contract_images() -> None:
    images_script = (SCRIPTS / "images.sh").read_text(encoding="utf-8")
    built = set(re.findall(r'"(btl/[a-z0-9-]+:dev)\|', images_script))
    assert built == set(CONTRACT_INFRA["images"])
    assert 'PLATFORM="${PLATFORM:-linux/arm64}"' in images_script


def test_env_file_is_read_not_executed(tmp_path: Path) -> None:
    marker = tmp_path / "executed"
    env_file = tmp_path / ".env"
    env_file.write_text(
        f'MINIO_ACCESS_KEY="abc"\nFRED_API_KEY=$(touch {marker})\n', encoding="utf-8"
    )
    command = (
        f"source {SCRIPTS / 'lib.sh'}; "
        "env_value MINIO_ACCESS_KEY; echo '|'; env_value FRED_API_KEY; echo '|'; env_value NOPE"
    )
    result = subprocess.run(
        ["bash", "-c", command],
        capture_output=True,
        text=True,
        env={"ENV_FILE": str(env_file), "PATH": "/usr/bin:/bin"},
    )
    assert result.returncode == 0, result.stderr
    access, fred, missing = (part.strip() for part in result.stdout.split("|"))
    assert access == "abc"
    assert fred == f"$(touch {marker})"
    assert missing == ""
    assert not marker.exists()


def test_secrets_reach_every_namespace_that_reads_minio() -> None:
    secrets = (SCRIPTS / "secrets.sh").read_text(encoding="utf-8")
    # kafka: role A's KafkaConnect S3 sink writes bronze objects to MinIO.
    assert "for namespace in minio kafka spark airflow market; do" in secrets


def test_up_waits_for_topics_before_role_workloads() -> None:
    up = (SCRIPTS / "up.sh").read_text(encoding="utf-8")
    # The S3 sink sends bad records to dead-letter; auto topic creation is off.
    assert up.index("kafkatopic --all --for=condition=Ready") < up.index("apply_role_dir")
