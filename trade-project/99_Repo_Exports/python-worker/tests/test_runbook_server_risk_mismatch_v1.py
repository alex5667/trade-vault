"""P4.8 test: verify runbook_server.py contains /api/risk-mismatch/latest endpoint."""
from pathlib import Path


def test_runbook_server_has_risk_mismatch_endpoint():
    """runbook_server.py must serve /api/risk-mismatch/latest (P4.8)."""
    src = (
        Path(__file__).resolve().parents[2]
        / 'runbooks'
        / 'server'
        / 'runbook_server.py'
    ).read_text(encoding='utf-8')
    assert '/api/risk-mismatch/latest' in src, \
        '/api/risk-mismatch/latest endpoint must be present in runbook_server.py'


def test_runbook_server_nav_bar_has_risk_drift():
    """The HTML index nav bar must include the Risk Drift JSON link."""
    src = (
        Path(__file__).resolve().parents[1]
        / 'runbooks'
        / 'server'
        / 'runbook_server.py'
    ).read_text(encoding='utf-8')
    assert 'Risk Drift JSON' in src, 'Nav bar must include Risk Drift JSON link'
