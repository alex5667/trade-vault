"""P4.6: Verify that the SQL migration files for risk decisions cagg exist."""
from pathlib import Path


def _root() -> Path:
    """Repository root: two levels above python-worker/tests/."""
    return Path(__file__).resolve().parents[2]


def test_risk_decision_summary_cagg_migration_exists() -> None:
    """CAGG SQL migration (20260306_11) must exist."""
    assert (
        _root() / 'python-worker' / 'db' / 'migrations' / '20260306_11_risk_decisions_cagg.sql'
    ).exists()
