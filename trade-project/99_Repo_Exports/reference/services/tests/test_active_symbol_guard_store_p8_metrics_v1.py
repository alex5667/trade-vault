from pathlib import Path
import importlib
import sys

root = Path(__file__).resolve().parents[2]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

metrics_mod = importlib.import_module('services.execution_metrics')
store_mod = importlib.import_module('services.active_symbol_guard_store')


class FakeRedis:
    def __init__(self):
        self.kv = {}

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None):
        self.kv[key] = value
        return True

    def scan_iter(self, match=None):
        prefix = str(match or '').rstrip('*')
        for key in list(self.kv.keys()):
            if key.startswith(prefix):
                yield key


ActiveSymbolGuardStore = store_mod.ActiveSymbolGuardStore


def _counter_value(metric, **labels):
    if metric is None:
        return 0.0
    return float(metric.labels(**labels)._value.get())


def test_resurrection_attempt_and_conflict_metrics_increment():
    r = FakeRedis()
    store = ActiveSymbolGuardStore(r, key_prefix='orders:active_symbol_sid:', active_ttl_sec=86400, tombstone_ttl_sec=120)

    before_res = _counter_value(metrics_mod.EXECUTION_ACTIVE_SYMBOL_GUARD_RESURRECTION_ATTEMPT_TOTAL, writer='projection', reason='released_tombstone_same_sid')
    before_conf = _counter_value(metrics_mod.EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_CONFLICT_TOTAL, writer='projection', operation='acquire_or_refresh', reason='released_tombstone_same_sid')

    first = store.acquire_or_refresh(symbol='BTCUSDT', sid='sid-A', payload_patch={'fsm_state': 'OPEN'}, writer='projection')
    assert first['applied'] is True
    store.mark_released(symbol='BTCUSDT', expected_sid='sid-A', release_reason='flat', writer='guard_repair')
    late = store.acquire_or_refresh(symbol='BTCUSDT', sid='sid-A', payload_patch={'fsm_state': 'OPEN_LATE'}, writer='projection')
    assert late['applied'] is False
    assert late['reason'] == 'released_tombstone_same_sid'

    after_res = _counter_value(metrics_mod.EXECUTION_ACTIVE_SYMBOL_GUARD_RESURRECTION_ATTEMPT_TOTAL, writer='projection', reason='released_tombstone_same_sid')
    after_conf = _counter_value(metrics_mod.EXECUTION_ACTIVE_SYMBOL_GUARD_CAS_CONFLICT_TOTAL, writer='projection', operation='acquire_or_refresh', reason='released_tombstone_same_sid')
    assert after_res >= before_res + 1
    assert after_conf >= before_conf + 1
