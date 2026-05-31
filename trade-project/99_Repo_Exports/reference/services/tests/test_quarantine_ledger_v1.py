from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / 'quarantine_ledger.py'
SPEC = importlib.util.spec_from_file_location('quarantine_ledger', SCRIPT)
mod = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = mod
SPEC.loader.exec_module(mod)


class _FakeCursor:
    def __init__(self, sink):
        self.sink = sink
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False
    def execute(self, sql, params):
        self.sink.append((sql, params))


class _FakeConn:
    def __init__(self):
        self.statements = []
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False
    def cursor(self):
        return _FakeCursor(self.statements)


def test_quarantine_ledger_records_event_and_run():
    sink = mod.QuarantineLedgerSink(dsn='postgresql://unused', connect_factory=lambda dsn: _FakeConn())
    assert sink.record_quarantine_event({'sid': 'sid-1', 'action': 'QUARANTINED', 'state': {'sid': 'sid-1'}}) is True
    assert sink.record_repair_run({'run_kind': 'automated_repair_policy', 'summary': {'ok': 1}}) is True
