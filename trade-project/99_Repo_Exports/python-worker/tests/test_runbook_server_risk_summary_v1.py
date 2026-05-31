"""P4.6: Verify that runbook_server.py exposes both risk API endpoints."""
from pathlib import Path


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_runbook_server_has_risk_canary_endpoint() -> None:
    """runbook_server.py must contain the /api/risk-canary/latest handler."""
    src = (
        _root() / 'python-worker' / 'runbooks' / 'server' / 'runbook_server.py'
    ).read_text(encoding='utf-8')
    assert '/api/risk-canary/latest' in src, (
        "runbook_server.py missing '/api/risk-canary/latest' endpoint"
    )


def test_runbook_server_has_risk_summary_endpoint() -> None:
    """runbook_server.py must contain the /api/risk-summary/latest handler."""
    src = (
        _root() / 'python-worker' / 'runbooks' / 'server' / 'runbook_server.py'
    ).read_text(encoding='utf-8')
    assert '/api/risk-summary/latest' in src, (
        "runbook_server.py missing '/api/risk-summary/latest' endpoint"
    )
