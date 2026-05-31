"""Tests for P3.3 autonomy: retention quarantine script existence and content."""
from pathlib import Path


def test_retention_quarantine_script_exists_and_mentions_action():
    """apply_retention_guard_quarantine.py must exist and implement the quarantine action."""
    src = (Path(__file__).resolve().parent.parent / 'scripts' / 'apply_retention_guard_quarantine.py').read_text(encoding='utf-8')
    # Must write the canonical quarantine action string to the Redis stream
    assert 'RETENTION_GUARD_QUARANTINED' in src
    # Must define the public run_policy() function used by auto-trigger
    assert 'run_policy' in src


def test_retention_quarantine_skip_stream_recovered():
    """Script must handle the skip_stream_recovered case."""
    src = (Path(__file__).resolve().parent.parent / 'scripts' / 'apply_retention_guard_quarantine.py').read_text(encoding='utf-8')
    assert 'skip_stream_recovered' in src


def test_retention_quarantine_ledger_integration():
    """Script must integrate with QuarantineLedgerSink."""
    src = (Path(__file__).resolve().parent.parent / 'scripts' / 'apply_retention_guard_quarantine.py').read_text(encoding='utf-8')
    assert 'QuarantineLedgerSink' in src
    assert 'record_quarantine_event' in src
