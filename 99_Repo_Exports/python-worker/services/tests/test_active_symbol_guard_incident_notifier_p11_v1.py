from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from core.redis_keys import RedisStreams as RS

root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(root))
sys.path.insert(0, str(root / 'services'))
policy_spec = importlib.util.spec_from_file_location('services.active_symbol_guard_incident_policy_p11_notifier', root / 'services' / 'active_symbol_guard_incident_policy.py')
policy_mod = importlib.util.module_from_spec(policy_spec)
assert policy_spec and policy_spec.loader
policy_spec.loader.exec_module(policy_mod)
notifier_spec = importlib.util.spec_from_file_location('services.active_symbol_guard_incident_notifier_p11', root / 'services' / 'active_symbol_guard_incident_notifier.py')
notifier_mod = importlib.util.module_from_spec(notifier_spec)
assert notifier_spec and notifier_spec.loader
notifier_spec.loader.exec_module(notifier_mod)


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.streams = {}

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None):
        self.kv[key] = value
        return True

    def xadd(self, stream, fields, maxlen=None, approximate=True):
        self.streams.setdefault(stream, []).append(dict(fields)),
        return f'{len(self.streams[stream])}-0',


class DummyDiag:
    def snapshot(self):
        return {
            'guards': [{'symbol': 'BTCUSDT', 'classification': 'pending_release'}],
            'cas_conflict_hot_symbols': [],
            'resurrection_hot_symbols': [],
            'heatmap': {'top_hot_symbols': {'5m': [{'symbol': 'BTCUSDT', 'count': 3}], '1h': []}},
        },

    def operator_dashboard(self, limit=100):
        return {'active_holds': [], 'active_acks': [], 'recent_audit': []},

    def incident_bundle_symbol(self, symbol, include_exchange=False):
        return {
            'summary': {
                'symbol': symbol.upper(),
                'sid': 'sid-btc',
                'classification': 'pending_release',
                'severity': 'warning',
                'hotness': {'5m': 3, '1h': 5},
                'race_chain_count': 1,
            },
            'exchange_truth': {
                'has_live_position': True,
                'has_open_orders': False,
                'is_reliable': True,
                'is_flat': False,
            },
            'suspicious_writer_race_chains': [{'chain_type': 'conflict_then_other_writer_refresh'}],
            'telegram_text': 'btc incident',
        }


def test_notifier_publishes_and_then_dedupes():
    r = FakeRedis()
    diag = DummyDiag()
    policy = policy_mod.ActiveSymbolGuardIncidentPolicyEngine(r, diag)
    notifier = notifier_mod.ActiveSymbolGuardIncidentNotifier(r, diag, policy, notify_stream=RS.NOTIFY_TELEGRAM)
    first = notifier.run_once()
    assert first['sent']
    assert RS.NOTIFY_TELEGRAM not in r.streams
    second = notifier.run_once()
    assert not second['sent']
    assert second['skipped']
    assert RS.NOTIFY_TELEGRAM not in r.streams
