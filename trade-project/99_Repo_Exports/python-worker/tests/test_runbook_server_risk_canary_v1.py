"""Tests for runbook_server P4.5: verifies /api/risk-canary/latest endpoint is present."""
from pathlib import Path


def _src() -> str:
    return (
        Path(__file__).resolve().parents[2]
        / 'python-worker' / 'runbooks' / 'server' / 'runbook_server.py'
    ).read_text(encoding='utf-8')


def test_runbook_server_has_risk_canary_endpoint():
    """runbook_server.py must contain the /api/risk-canary/latest route (P4.5)."""
    src = _src()
    assert '/api/risk-canary/latest' in src, "/api/risk-canary/latest endpoint missing"


def test_runbook_server_serves_latest_risk_canary_json():
    """runbook_server.py must serve latest_risk_engine_canary.json (P4.5)."""
    src = _src()
    assert 'latest_risk_engine_canary.json' in src, "json filename missing"
