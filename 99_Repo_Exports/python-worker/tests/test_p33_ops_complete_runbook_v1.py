from __future__ import annotations

"""P3.3-ops-complete test: runbook server /api/rebuild/latest endpoint.

Verifies that:
- The server source exposes /api/rebuild/latest
- The ops-complete runbook file exists
"""

from pathlib import Path


def test_runbook_server_exposes_rebuild_endpoint():
    """runbook_server.py must handle /api/rebuild/latest."""
    src = (
        Path(__file__).resolve().parents[1] / 'runbooks' / 'server' / 'runbook_server.py'
    ).read_text(encoding='utf-8')
    assert '/api/rebuild/latest' in src, "runbook_server must expose /api/rebuild/latest"


def test_ops_complete_runbook_exists():
    """P33_OPS_COMPLETE_REPLAY.md runbook must exist."""
    runbook = Path(__file__).resolve().parents[1] / 'runbooks' / 'P33_OPS_COMPLETE_REPLAY.md'
    assert runbook.exists(), f"Missing runbook: {runbook}"
