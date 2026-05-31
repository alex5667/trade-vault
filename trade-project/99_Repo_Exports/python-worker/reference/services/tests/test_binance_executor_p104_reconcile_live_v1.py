import importlib.util
import json
import sys
from pathlib import Path

mod_path = Path(__file__).parent.parent / 'binance_executor.py'
spec = importlib.util.spec_from_file_location('binance_executor_p104', mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.stream = []

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None):
        self.kv[key] = value

    def xadd(self, key, fields):
        self.stream.append((key, dict(fields)))


class DummyClient:
    def __init__(self):
        self.called = []

    def reconcile_entry_by_client_id(self, symbol, client_order_id):
        self.called.append(('plain', symbol, client_order_id))
        return {'orderId': 1, 'clientOrderId': client_order_id, 'status': 'FILLED', 'executedQty': '1'}

    def query_algo_order(self, symbol, client_algo_id=None):
        self.called.append(('algo', symbol, client_algo_id))
        return {'algoId': 2, 'clientAlgoId': client_algo_id, 'status': 'NEW'}

    def get_open_algo_orders(self, symbol):
        return []



def _mk_exec():
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    ex.r = FakeRedis()
    ex.exec_stream = 'orders:exec'
    ex.user_stream_cache_prefix = 'orders:user_stream:'
    ex.user_stream_status_key = 'orders:user_stream:status'
    ex.user_stream_max_stale_ms = 45000
    ex.exec_require_user_stream_live = True
    ex.execution_journal = None
    return ex


def test_user_stream_live_guard_reads_status_key():
    ex = _mk_exec()
    ex.r.set('orders:user_stream:status', json.dumps({'last_event_ms': mod._ms_now()}))
    assert ex._user_stream_is_live() is True


def test_reconcile_uses_user_stream_cache_first():
    ex = _mk_exec()
    ex.r.set('orders:user_stream:order:cid-1', json.dumps({'order': {'i': 55, 'c': 'cid-1', 'X': 'FILLED', 'z': '1'}}))
    out = ex._reconcile_execution_status(
        sid='sid-1',
        symbol='BTCUSDT',
        action='open',
        client=DummyClient(),
        plain_client_id='cid-1',
    )
    assert out['orderId'] == 55
    assert out['clientOrderId'] == 'cid-1'
