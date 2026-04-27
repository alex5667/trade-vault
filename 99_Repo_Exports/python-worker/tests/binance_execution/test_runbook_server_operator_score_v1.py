"""P4.7: Verify that the runbook server has the /api/operator-score/latest endpoint."""
from pathlib import Path


def test_runbook_server_has_operator_score_endpoint():
    """/api/operator-score/latest must be present in runbook_server.py."""
    src = (
        Path(__file__).resolve().parents[2]
        / 'runbooks'
        / 'server'
        / 'runbook_server.py'
    ).read_text(encoding='utf-8')
    assert '/api/operator-score/latest' in src
