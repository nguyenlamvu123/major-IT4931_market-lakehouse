"""GitHub settings live at the git root, where GitHub actually reads them."""

from __future__ import annotations

from pathlib import Path

import yaml

PROJECT = Path(__file__).resolve().parents[2]
GIT_ROOT = PROJECT.parent
GITHUB = GIT_ROOT / ".github"


def test_workflow_is_at_git_root_not_inside_project() -> None:
    # Regression: btl-market-lakehouse/.github/workflows/ci.yml was never run by GitHub.
    assert (GITHUB / "workflows" / "ci.yml").exists()
    assert not (PROJECT / ".github").exists()


def test_ci_runs_ruff_and_pytest_in_project_dir() -> None:
    workflow = yaml.safe_load((GITHUB / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    assert workflow["defaults"]["run"]["working-directory"] == PROJECT.name
    commands = " ".join(
        step.get("run", "") for job in workflow["jobs"].values() for step in job["steps"]
    )
    for command in ("ruff check .", "python -m pytest", "docker compose", "kubeconform"):
        assert command in commands, command


def test_codeowners_cover_role_folders_and_shared_contract() -> None:
    lines = [
        line.split()
        for line in (GITHUB / "CODEOWNERS").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]
    owners = {parts[0]: parts[1:] for parts in lines}
    prefix = f"/{PROJECT.name}"
    assert owners[f"{prefix}/ingest/"] == ["@ban_A"]
    assert owners[f"{prefix}/streaming/"] == ["@ban_B"]
    assert owners[f"{prefix}/batch/"] == ["@ban_C"]
    assert owners[f"{prefix}/infra/"] == ["@ban_D"]
    everyone = ["@ban_A", "@ban_B", "@ban_C", "@ban_D"]
    assert owners[f"{prefix}/contracts/"] == everyone
    assert owners[f"{prefix}/common/"] == everyone
    for path in owners:
        target = GIT_ROOT / path.strip("/")
        assert target.exists() or path in {f"{prefix}/{name}/" for name in ("ingest", "streaming", "batch")}, path


def test_pull_request_template_has_pdf_checklist() -> None:
    template = (GITHUB / "pull_request_template.md").read_text(encoding="utf-8")
    for needle in ("Closes #", "contracts/", ".env", "data/", "CI"):
        assert needle in template, needle
