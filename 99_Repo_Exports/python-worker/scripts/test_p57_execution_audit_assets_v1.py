from __future__ import annotations
"""P5.7 asset integrity tests: verify systemd units and runbook reference the expected SoT paths."""

from pathlib import Path


# Root of the python-worker subtree (two levels up from scripts/)
ROOT = Path(__file__).resolve().parents[1]
# Repository root (one more level up)
REPO_ROOT = ROOT.parent


def test_systemd_units_exist_and_reference_expected_paths() -> None:
    """All four P5.7 systemd assets must exist and contain correct SoT path references."""
    service = (REPO_ROOT / "deploy" / "systemd" / "execution-audit-chain-check.service").read_text(encoding="utf-8")
    timer = (REPO_ROOT / "deploy" / "systemd" / "execution-audit-chain-check.timer").read_text(encoding="utf-8")
    runbook_srv = (REPO_ROOT / "deploy" / "systemd" / "execution-audit-runbook-server.service").read_text(encoding="utf-8")
    env_example = (REPO_ROOT / "deploy" / "systemd" / "execution-audit-chain.env.example").read_text(encoding="utf-8")

    # Service must invoke the P5.6 checker script
    assert "check_execution_audit_chain.py" in service, "service should invoke P5.6 checker"
    # SoT paths in the service
    assert "/var/lib/node_exporter/textfile_collector/latest_execution_audit_chain.prom" in service
    assert "/var/lib/trade/runbooks/latest_execution_audit_chain.json" in service
    # Timer cadence
    assert "OnUnitActiveSec=5min" in timer, "timer should fire every 5 minutes"
    # Runbook server must reference the server script
    assert "runbook_server.py" in runbook_srv
    # Env example must define the JSON SoT path
    assert "EXEC_AUDIT_REPORT_JSON=/var/lib/trade/runbooks/latest_execution_audit_chain.json" in env_example


def test_p57_runbook_mentions_compose_and_systemd() -> None:
    """P5.7 runbook must document both systemd timer and compose service wiring."""
    text = (ROOT / "runbooks" / "P57_AUDIT_CHAIN_TIMER_AND_COMPOSE.md").read_text(encoding="utf-8")
    assert "execution-audit-chain-check.timer" in text, "runbook must reference systemd timer"
    assert "execution-audit-chain-checker" in text, "runbook must reference compose service"


def test_scheduler_script_exists() -> None:
    """P5.7 scheduler script must be present in scripts/ next to the P5.6 checker."""
    scheduler = ROOT / "scripts" / "run_execution_audit_chain_scheduler.py"
    assert scheduler.is_file(), f"scheduler not found: {scheduler}"
    text = scheduler.read_text(encoding="utf-8")
    assert "check_execution_audit_chain" in text, "scheduler must reference P5.6 checker"
