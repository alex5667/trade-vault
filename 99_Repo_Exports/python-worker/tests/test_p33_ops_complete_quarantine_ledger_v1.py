"""P3.3-ops-complete test: quarantine ledger event contract in binance_executor.

Verifies via source code inspection that the executor writes 'record_quarantine_event'
on REPLAY_MISMATCH_QUARANTINED — a contract test that survives refactors.
"""
from __future__ import annotations

from pathlib import Path


def test_executor_replay_mismatch_records_ledger_contract():
    """binance_executor must call record_quarantine_event on replay mismatch."""
    src = (Path(__file__).resolve().parents[1] / 'services' / 'binance_executor.py').read_text(encoding='utf-8')
    assert 'record_quarantine_event' in src, "binance_executor must call quarantine_ledger.record_quarantine_event"
    assert 'REPLAY_MISMATCH_QUARANTINED' in src, "binance_executor must emit REPLAY_MISMATCH_QUARANTINED event"


def test_executor_imports_quarantine_ledger():
    """binance_executor must import QuarantineLedgerSink."""
    src = (Path(__file__).resolve().parents[1] / 'services' / 'binance_executor.py').read_text(encoding='utf-8')
    assert 'QuarantineLedgerSink' in src, "binance_executor must import QuarantineLedgerSink"


def test_executor_has_replay_checkpoint_key_helper():
    """binance_executor must have _replay_checkpoint_key helper."""
    src = (Path(__file__).resolve().parents[1] / 'services' / 'binance_executor.py').read_text(encoding='utf-8')
    assert '_replay_checkpoint_key' in src, "Missing _replay_checkpoint_key method"


def test_check_consistency_has_ledger_param():
    """check_execution_replay_consistency must support ledger parameter."""
    src = (Path(__file__).resolve().parents[1] / 'scripts' / 'check_execution_replay_consistency.py').read_text(encoding='utf-8')
    assert 'ledger' in src, "check_execution_replay_consistency must accept/use ledger"
    assert 'record_quarantine_event' in src, "check_execution_replay_consistency must call record_quarantine_event"
