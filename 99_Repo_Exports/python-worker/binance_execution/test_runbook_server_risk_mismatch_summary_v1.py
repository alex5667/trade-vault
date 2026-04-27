from pathlib import Path


def test_runbook_server_has_risk_mismatch_summary_endpoint():
    """P4.9: verify runbook_server exposes /api/risk-mismatch-summary/latest endpoint."""
    src = (Path(__file__).resolve().parents[1] / 'runbooks' / 'server' / 'runbook_server.py').read_text(encoding='utf-8')
    assert '/api/risk-mismatch-summary/latest' in src
