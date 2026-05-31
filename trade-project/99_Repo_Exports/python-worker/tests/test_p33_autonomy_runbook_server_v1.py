"""Tests for P3.3 autonomy: runbook server endpoint exposure."""
from pathlib import Path


def test_runbook_server_exposes_replay_slo_and_autonomy():
    """runbook_server.py must define both new P3.3-autonomy endpoints."""
    src = (Path(__file__).resolve().parent.parent / 'runbooks' / 'server' / 'runbook_server.py').read_text(encoding='utf-8')
    assert '/api/autonomy/latest' in src
    assert '/api/replay-slo/latest' in src


def test_runbook_server_autonomy_reads_correct_report():
    """Autonomy endpoint must serve latest_auto_scrubber.json."""
    src = (Path(__file__).resolve().parent.parent / 'runbooks' / 'server' / 'runbook_server.py').read_text(encoding='utf-8')
    assert 'latest_auto_scrubber.json' in src


def test_runbook_server_replay_slo_reads_correct_report():
    """Replay SLO endpoint must serve latest_replay_slo_summary.json."""
    src = (Path(__file__).resolve().parent.parent / 'runbooks' / 'server' / 'runbook_server.py').read_text(encoding='utf-8')
    assert 'latest_replay_slo_summary.json' in src
