"""P4.7: Verify that the SQL migration files for risk audit retention partitioning exist."""
from pathlib import Path


def _root() -> Path:
    """Repository root: two levels above python-worker/tests/."""
    return Path(__file__).resolve().parents[2]


def test_risk_retention_migration_exists() -> None:
    """Partitioning + archive table SQL migration (20260306_13) must exist."""
    assert (
        _root() / 'python-worker' / 'db' / 'migrations'
        / '20260306_13_risk_audit_retention_partitioning.sql'
    ).exists()


def test_risk_retention_indexes_migration_exists() -> None:
    """Index SQL migration (20260306_14) for archive tables must exist."""
    assert (
        _root() / 'python-worker' / 'db' / 'migrations'
        / '20260306_14_risk_audit_retention_partitioning_indexes.sql'
    ).exists()


def test_purge_script_exists() -> None:
    """Purge script called by nightly retention timer must exist."""
    assert (
        _root() / 'python-worker' / 'scripts' / 'purge_risk_audit_hot_tables.py'
    ).exists()
