from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / 'scripts' / 'automated_execution_repair_policy.py'
SPEC = importlib.util.spec_from_file_location('automated_execution_repair_policy', SCRIPT)
mod = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = mod
SPEC.loader.exec_module(mod)


class _Summary:
    def __init__(self, critical: int, mismatches=None):
        self.critical_mismatches = critical
        self.mismatches = mismatches or []
    def to_dict(self):
        return {'critical_mismatches': self.critical_mismatches, 'mismatches': self.mismatches}


def test_run_policy_repairs_when_under_threshold(monkeypatch):
    calls = {'repair': 0, 'quarantine': 0}
    seq = [_Summary(1, []), _Summary(0, [])]
    monkeypatch.setattr(mod.consistency, 'run_check', lambda **kwargs: seq.pop(0))
    monkeypatch.setattr(mod.repair_mod, 'run_repair', lambda **kwargs: calls.__setitem__('repair', calls['repair'] + 1) or {'applied': True})
    monkeypatch.setattr(mod.quarantine_mod, 'build_quarantine_targets', lambda *args, **kwargs: [])
    result = mod.run_policy(redis_url='redis://', journal_dsn='dsn', state_prefix='orders:state:', exec_stream='orders:exec', stream_count=10, max_auto_repair_critical=2, quarantine_min_severity='critical', dry_run=True, ledger_dsn='')
    assert calls['repair'] == 1
    assert result['repaired']['applied'] is True
