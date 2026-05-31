"""Tests verifying that all P4.5 static assets exist at the expected paths."""
from pathlib import Path


def _root() -> Path:
    """Repository root: parent of python-worker."""
    return Path(__file__).resolve().parents[2]


def test_prometheus_rules_p45_exists():
    """Prometheus alert rules file must exist."""
    assert (_root() / 'monitoring' / 'prometheus_rules_execution_p45_risk.yml').exists()


def test_grafana_dashboard_p45_exists():
    """Grafana dashboard JSON must exist."""
    assert (
        _root() / 'monitoring' / 'grafana' / 'dashboards' / 'trade_execution_p45_risk_quality.json'
    ).exists()


def test_canary_report_script_exists():
    """Canary report builder script must exist."""
    assert (_root() / 'python-worker' / 'scripts' / 'build_risk_engine_canary_report.py').exists()


def test_migration_09_exists():
    """DB migration 09 (risk_decisions schema) must exist."""
    assert (
        _root() / 'python-worker' / 'db' / 'migrations' / '20260306_09_risk_decisions.sql'
    ).exists()


def test_migration_10_exists():
    """DB migration 10 (risk audit indexes) must exist."""
    assert (
        _root() / 'python-worker' / 'db' / 'migrations' / '20260306_10_risk_decisions_indexes.sql'
    ).exists()


def test_risk_audit_sql_sink_exists():
    """risk_audit_sql.py module must exist in services/risk/."""
    assert (
        _root() / 'python-worker' / 'services' / 'risk' / 'risk_audit_sql.py'
    ).exists()
