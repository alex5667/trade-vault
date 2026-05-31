from pathlib import Path
import importlib
import json
import sys
import time

root = Path(__file__).resolve().parents[2]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

diag_mod = importlib.import_module('services.active_symbol_guard_diagnostics')
store_mod = importlib.import_module('services.active_symbol_guard_store')


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.hashes = {}

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

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[str(field)] = value
        return 1

    def hincrby(self, key, field, amount=1):
        bucket = self.hashes.setdefault(key, {})
        bucket[str(field)] = int(bucket.get(str(field), 0)) + int(amount)
        return bucket[str(field)]


ActiveSymbolGuardStore = store_mod.ActiveSymbolGuardStore
ActiveSymbolGuardDiagnostics = diag_mod.ActiveSymbolGuardDiagnostics


def _rewrite_ts(store, symbol, offsets_ms):
    now_ms = int(time.time() * 1000)
    gkey = store._timeline_key()
    skey = store._symbol_timeline_key(symbol)
    for key in (gkey, skey):
        items = json.loads(store.r.get(key))
        idx = 0
        for item in items:
            if str(item.get('symbol') or '').upper() != str(symbol).upper():
                continue
            if idx >= len(offsets_ms):
                break
            item['ts_ms'] = now_ms - int(offsets_ms[idx])
            idx += 1
        store.r.set(key, json.dumps(items))


def test_heatmap_rolls_5m_and_1h_windows():
    r = FakeRedis()
    store = ActiveSymbolGuardStore(r)
    for _ in range(3):
        store._record_conflict(symbol='BTCUSDT', writer='executor', operation='acquire_or_refresh', reason='held_by_other_sid')
    for _ in range(2):
        store._record_conflict(symbol='ETHUSDT', writer='projection', operation='acquire_or_refresh', reason='held_by_other_sid')
    store._record_conflict(symbol='ETHUSDT', writer='projection', operation='acquire_or_refresh', reason='version_mismatch')
    # Push ETH events outside 5m but within 1h.
    _rewrite_ts(store, 'ETHUSDT', [700_000, 900_000, 1_500_000])

    diag = ActiveSymbolGuardDiagnostics(r, hot_symbol_limit=5)
    heatmap = diag.heatmap()
    hot_5m = heatmap['top_hot_symbols']['5m']
    hot_1h = heatmap['top_hot_symbols']['1h']
    assert hot_5m[0]['symbol'] == 'BTCUSDT'
    assert hot_5m[0]['count'] == 3
    assert all(item['symbol'] != 'ETHUSDT' for item in hot_5m)
    assert hot_1h[0]['symbol'] == 'BTCUSDT'
    assert any(item['symbol'] == 'ETHUSDT' and item['count'] == 3 for item in hot_1h)


def test_incident_bundle_contains_timeline_race_chains_and_payloads():
    r = FakeRedis()
    store = ActiveSymbolGuardStore(r)
    store.acquire_or_refresh(symbol='BTCUSDT', sid='sid-a', payload_patch={'fsm_state': 'OPEN'}, writer='projection')
    store._record_conflict(symbol='BTCUSDT', writer='projection', operation='acquire_or_refresh', reason='held_by_other_sid')
    store.acquire_or_refresh(symbol='BTCUSDT', sid='sid-a', payload_patch={'fsm_state': 'OPEN'}, writer='executor')
    r.set('orders:state:sid-a', json.dumps({'sid': 'sid-a', 'symbol': 'BTCUSDT', 'fsm_state': 'OPEN'}))

    diag = ActiveSymbolGuardDiagnostics(r, hot_symbol_limit=5)
    bundle = diag.incident_bundle_symbol('BTCUSDT', include_exchange=False)
    assert bundle['summary']['symbol'] == 'BTCUSDT'
    assert bundle['summary']['hotness']['5m'] >= 1
    assert bundle['last_writer_timeline']
    assert bundle['suspicious_writer_race_chains']
    assert 'BTCUSDT' in bundle['telegram_text']
    assert bundle['http_payload']['timeline']
    assert bundle['ui_payload']['card']['symbol'] == 'BTCUSDT'
