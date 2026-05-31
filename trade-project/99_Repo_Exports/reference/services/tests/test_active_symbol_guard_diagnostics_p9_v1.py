from pathlib import Path
import importlib
import json
import sys
import time

root = Path(__file__).resolve().parents[2]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

mod = importlib.import_module('services.active_symbol_guard_diagnostics')
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


class DummyClient:
    def get_position_risk(self):
        return [{'symbol': 'BTCUSDT', 'positionAmt': '0.50'}]

    def get_open_orders(self, symbol):
        return [{'symbol': symbol}] if symbol == 'BTCUSDT' else []

    def get_open_algo_orders(self, symbol):
        return []


def test_snapshot_breakdown_and_hot_symbols():
    r = FakeRedis()
    store = store_mod.ActiveSymbolGuardStore(r)
    now_ms = int(time.time() * 1000)

    store.acquire_or_refresh(symbol='BTCUSDT', sid='sid-a', payload_patch={'guard_release_pending': False}, writer='executor')
    store.acquire_or_refresh(symbol='ETHUSDT', sid='sid-b', payload_patch={'guard_release_pending': True, 'state_terminalish': True}, writer='projection')
    r.set('orders:active_symbol_sid:SOLUSDT', json.dumps({
        'symbol': 'SOLUSDT', 'sid': 'sid-c', 'guard_status': 'released', 'released_at_ms': now_ms - 30_000, 'guard_version': 2,
    }))
    r.set('orders:active_symbol_sid:XRPUSDT', json.dumps({
        'symbol': 'XRPUSDT', 'sid': 'sid-d', 'guard_status': 'released', 'released_at_ms': now_ms - 700_000, 'guard_version': 4,
    }))
    store._record_conflict(symbol='ETHUSDT', writer='projection', operation='acquire_or_refresh', reason='held_by_other_sid')
    store._record_conflict(symbol='ETHUSDT', writer='projection', operation='acquire_or_refresh', reason='held_by_other_sid')
    store._record_conflict(symbol='BTCUSDT', writer='executor', operation='mark_released', reason='version_mismatch')

    diag = mod.ActiveSymbolGuardDiagnostics(r, stale_tombstone_ms=600_000, hot_symbol_limit=5)
    snap = diag.snapshot()
    assert snap['ok'] is True
    assert snap['breakdown']['active'] == 1
    assert snap['breakdown']['pending_release'] == 1
    assert snap['breakdown']['released_tombstone'] == 1
    assert snap['breakdown']['stale_tombstone'] == 1
    assert snap['degraded'] is True
    assert snap['cas_conflict_hot_symbols'][0]['symbol'] == 'ETHUSDT'
    assert snap['cas_conflict_hot_symbols'][0]['count'] == 2


def test_debug_symbol_and_sid_include_state_and_exchange_truth():
    r = FakeRedis()
    store = store_mod.ActiveSymbolGuardStore(r)
    store.acquire_or_refresh(symbol='BTCUSDT', sid='sid-1', payload_patch={'fsm_state': 'OPEN'}, writer='executor')
    r.set('orders:state:sid-1', json.dumps({'sid': 'sid-1', 'symbol': 'BTCUSDT', 'fsm_state': 'OPEN'}))
    diag = mod.ActiveSymbolGuardDiagnostics(r, client=DummyClient())

    by_symbol = diag.debug_symbol('BTCUSDT', include_exchange=True)
    assert by_symbol['guard_view']['sid'] == 'sid-1'
    assert by_symbol['state']['fsm_state'] == 'OPEN'
    assert by_symbol['exchange_truth']['has_live_position'] is True

    by_sid = diag.debug_sid('sid-1', include_exchange=True)
    assert by_sid['symbol'] == 'BTCUSDT'
    assert by_sid['guard_view']['is_active'] is True
    assert by_sid['exchange_truth']['open_plain_orders'] == 1
