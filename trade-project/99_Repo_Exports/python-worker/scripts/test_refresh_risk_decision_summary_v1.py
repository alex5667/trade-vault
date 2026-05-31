"""P4.6: Verify that both risk summary scripts exist in the scripts directory."""
from pathlib import Path


def _scripts() -> Path:
    return Path(__file__).resolve().parent


def test_refresh_risk_decision_summary_script_exists() -> None:
    """refresh_risk_decision_summary.py must exist in scripts/."""
    assert (_scripts() / 'refresh_risk_decision_summary.py').exists(), (
        "scripts/refresh_risk_decision_summary.py not found"
    )


def test_check_risk_signal_snapshot_consistency_script_exists() -> None:
    """check_risk_signal_snapshot_consistency.py must exist in scripts/."""
    assert (_scripts() / 'check_risk_signal_snapshot_consistency.py').exists(), (
        "scripts/check_risk_signal_snapshot_consistency.py not found"
    )
