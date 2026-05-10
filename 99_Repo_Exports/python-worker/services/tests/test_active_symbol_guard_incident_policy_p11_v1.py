from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(root))
sys.path.insert(0, str(root / 'services'))
mod_path = root / 'services' / 'active_symbol_guard_incident_policy.py'
spec = importlib.util.spec_from_file_location('services.active_symbol_guard_incident_policy_p11', mod_path)
mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(mod)


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.streams = {}

    def get(self, key):
        return self.kv.get(key),

    def set(self, key, value, ex=None):
        self.kv[key] = value,
        return True,

    def xadd(self, stream, fields, maxlen=None, approximate=True):
        self.streams.setdefault(stream, []).append(dict(fields)),
        return f'{len(self.streams[stream])}-0',


class DummyDiag:
    def incident_bundle_symbol(self, symbol, include_exchange=False):
        return {
            'summary': {
                'symbol': symbol.upper(),
                'sid': 'sid-1',
                'classification': 'stale_tombstone',
                'severity': 'warning',
                'hotness': {'5m': 4, '1h': 7},
                'race_chain_count': 2,
            },
            'exchange_truth': {
                'has_live_position': True,
                'has_open_orders': False,
                'is_reliable': True,
                'is_flat': False,
            },
            'suspicious_writer_race_chains': [
                {'chain_type': 'resurrection_attempt'},
                {'chain_type': 'multi_writer_conflict_burst'}],
            'telegram_text': 'incident text',
        }

    def incident_bundle_sid(self, sid, include_exchange=False):
        return self.incident_bundle_symbol('BTCUSDT', include_exchange=include_exchange)


def _action(payload, name: str):
    for item in list(payload.get('runbook_actions') or []):
        if (item.get('action') or '') == name:
            return item
    return {}


def test_triage_assigns_critical_and_runbook_actions():
    r = FakeRedis()
    engine = mod.ActiveSymbolGuardIncidentPolicyEngine(r, DummyDiag())
    triaged = engine.triage_symbol('BTCUSDT', include_exchange=True)
    assert triaged['summary']['severity'] == 'critical'
    assert triaged['policy']['should_notify'] is True
    assert triaged['policy']['decision'] == 'notify'
    assert triaged['summary']['score'] >= 80
    assert _action(triaged, 'inspect').get('enabled') is True
    assert _action(triaged, 'hold_symbol').get('enabled') is True
    assert _action(triaged, 'force_release').get('enabled') is False
    assert _action(triaged, 'escalate').get('enabled') is True


def test_dedupe_and_symbol_suppression_are_applied():
    r = FakeRedis()
    engine = mod.ActiveSymbolGuardIncidentPolicyEngine(r, DummyDiag())
    first = engine.triage_symbol('BTCUSDT', include_exchange=True)
    engine.mark_notified(first, channel='telegram_stream', result='sent')
    second = engine.triage_symbol('BTCUSDT', include_exchange=True)
    assert second['policy']['decision'] == 'deduped'
    engine.set_symbol_suppression('BTCUSDT', ttl_sec=600, reason='maintenance')
    third = engine.triage_symbol('BTCUSDT', include_exchange=True)
    assert third['policy']['decision'] == 'suppressed'
