from pathlib import Path
import importlib
import json
import sys
import time

root = Path(__file__).resolve().parents[2]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

metrics_mod = importlib.import_module('services.execution_metrics')
worker_mod = importlib.import_module('services.binance_active_symbol_guard_repair_worker')


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


class DummyClient:
    def get_position_risk(self):
        return []

    def get_open_orders(self, symbol):
        return []

    def get_open_algo_orders(self, symbol):
        return []


def test_repair_worker_exports_released_tombstone_age_metric():
    r = FakeRedis()
    now_ms = int(time.time() * 1000)
    r.set('orders:active_symbol_sid:BTCUSDT', json.dumps({
        'symbol': 'BTCUSDT',
        'sid': 'sid-old',
        'guard_status': 'released',
        'released_at_ms': now_ms - 15_000,
        'guard_version': 3,
    }))
    worker = worker_mod.BinanceActiveSymbolGuardRepairWorker(redis_client=r, client=DummyClient())
    out = worker.run_once()
    assert out and out[0]['status'] == 'released_tombstone'
    value = float(metrics_mod.EXECUTION_ACTIVE_SYMBOL_GUARD_RELEASED_TOMBSTONE_AGE_MS.labels(symbol='BTCUSDT')._value.get())
    assert value >= 10_000
