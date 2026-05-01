from __future__ import annotations
"""P5.7 compose wiring tests: verify that docker-compose-timers.yml contains the audit services."""

from pathlib import Path


# Root of the python-worker subtree (two levels up from scripts/)
ROOT = Path(__file__).resolve().parents[1]
# Repository root (one more level up)
REPO_ROOT = ROOT.parent


def test_compose_contains_execution_audit_services() -> None:
    """docker-compose-timers.yml must declare both P5.7 execution audit services."""
    text = (REPO_ROOT / "docker-compose-timers.yml").read_text(encoding="utf-8")
    assert "execution-audit-chain-checker:" in text, "checker service missing from timers compose"
    assert "execution-audit-runbook-server:" in text, "runbook server service missing from timers compose"


def test_compose_execution_audit_has_correct_prom_path() -> None:
    """P5.7 checker service must write .prom to the SoT textfile-collector path."""
    text = (REPO_ROOT / "docker-compose-timers.yml").read_text(encoding="utf-8")
    assert (
        "EXEC_AUDIT_REPORT_PROM=/var/lib/node_exporter/textfile_collector/latest_execution_audit_chain.prom"
        in text
    ), "wrong or missing EXEC_AUDIT_REPORT_PROM in compose"


def test_compose_execution_audit_bind_mounts_runtime_dir() -> None:
    """P5.7 services must use bind-mounted runtime/ dirs for output artifacts."""
    text = (REPO_ROOT / "docker-compose-timers.yml").read_text(encoding="utf-8")
    assert "./runtime/execution_audit_chain:/var/lib/trade/runbooks" in text, \
        "bind mount for JSON report missing from compose"
    assert "./runtime/node_exporter_textfile:/var/lib/node_exporter/textfile_collector" in text, \
        "bind mount for .prom file missing from compose"


def test_compose_execution_audit_uses_ops_profile() -> None:
    """P5.7 services must be gated behind the 'ops' profile so they don't start by default."""
    text = (REPO_ROOT / "docker-compose-timers.yml").read_text(encoding="utf-8")
    # Both services must appear, and 'ops' profile must appear at least twice
    assert text.count('profiles: ["ops"]') >= 2, \
        "both P5.7 services should have profiles: [\"ops\"]"


def test_compose_execution_audit_scheduler_command() -> None:
    """P5.7 checker service must invoke the P5.7 scheduler script (not a bare shell loop)."""
    text = (REPO_ROOT / "docker-compose-timers.yml").read_text(encoding="utf-8")
    assert "run_execution_audit_chain_scheduler.py" in text, \
        "checker service must invoke the scheduler script"
