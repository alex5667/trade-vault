"""P5X test: verify runbook server has both new P5X API endpoints."""
from pathlib import Path


def test_runbook_server_has_new_endpoints():
    """runbook_server.py must expose both P5X API endpoints."""
    src = (Path(__file__).resolve().parents[1] / 'runbooks' / 'server' / 'runbook_server.py').read_text(encoding='utf-8')
    assert '/api/risk-mismatch-archive-consistency/latest' in src, \
        'Missing /api/risk-mismatch-archive-consistency/latest endpoint'
    assert '/api/risk-drift-autosilence/latest' in src, \
        'Missing /api/risk-drift-autosilence/latest endpoint'
