from pathlib import Path
import importlib.util
import json
import sys

# ---------------------------------------------------------------------------
# P6 test: Exchange Truth Guard Semantics — projection worker, executor, repair
# ---------------------------------------------------------------------------
# Tests that projection worker, inline executor, and repair worker all write
# the same unified guard lifecycle contract when exchange_truth_release=True.
#
# Covered:
#   1. projection worker keeps terminal guard as pending-release (exchange_truth_release=1)
#   2. projection worker deletes terminal guard in legacy mode (exchange_truth_release=0)
#   3. inline executor and projection worker write identical pending-release contract
#   4. repair worker correctly releases pending-release guard after exchange flat
# ---------------------------------------------------------------------------

root = Path(__file__).resolve().parents[2]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

exec_mod_path = root / 'services' / 'binance_executor.py'
exec_spec = importlib.util.spec_from_file_location('services.binance_executor_p6_guard_semantics', exec_mod_path)
exec_mod = importlib.util.module_from_spec(exec_spec)
sys.modules[exec_spec.name] = exec_mod
assert exec_spec.loader is not None
exec_spec.loader.exec_module(exec_mod)

worker_mod_path = root / 'services' / 'execution_projection_worker.py'
worker_spec = importlib.util.spec_from_file_location('services.execution_projection_worker_p6_guard_semantics', worker_mod_path)
worker_mod = importlib.util.module_from_spec(worker_spec)
sys.modules[worker_spec.name] = worker_mod
assert worker_spec.loader is not None
worker_spec.loader.exec_module(worker_mod)

repair_mod_path = root / 'services' / 'binance_active_symbol_guard_repair_worker.py'
repair_spec = importlib.util.spec_from_file_location('services.binance_guard_repair_p6_guard_semantics', repair_mod_path)
repair_mod = importlib.util.module_from_spec(repair_spec)
sys.modules[repair_spec.name] = repair_mod
assert repair_spec.loader is not None
repair_spec.loader.exec_module(repair_mod)


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.streams = {}
        self._seq = 0

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None):
        self.kv[key] = value

    def delete(self, key):
        self.kv.pop(key, None)

    def xadd(self, key, fields, maxlen=None, approximate=None):
        self._seq += 1
        sid = f"{1700000000000 + self._seq}-0"
        self.streams.setdefault(key, []).append((sid, dict(fields)))
        return sid

    def xrange(self, key, start='-', end='+', count=None):
        rows = list(self.streams.get(key, []))
        if start not in ('', '-'):
            exclusive = start.startswith('(')
            target = start[1:] if exclusive else start
            rows = [row for row in rows if (row[0] > target if exclusive else row[0] >= target)]
        if count is not None:
            rows = rows[: int(count)]
        return rows

    def xrevrange(self, key, start='+', end='-', count=None):
        rows = list(reversed(self.streams.get(key, [])))
        if count is not None:
            rows = rows[: int(count)]
        return rows

    def scan_iter(self, match=None):
        prefix = str(match or '').rstrip('*')
        for key in list(self.kv.keys()):
            if key.startswith(prefix):
                yield key


class FlatClient:
    def get_position_risk(self):
        return [{'symbol': 'BTCUSDT', 'positionAmt': '0'}]
    def get_open_orders(self, symbol=None):
        return []
    def get_open_algo_orders(self, symbol=None):
        return []


def _mk_inline_exec(redis_obj):
    """Build a minimal BinanceExecutor shell for testing guard writes."""
    ex = exec_mod.BinanceExecutor.__new__(exec_mod.BinanceExecutor)
    ex.r = redis_obj
    ex.exec_single_active_position_per_symbol = True
    ex.exec_single_active_position_release_on_terminal = True
    ex.exec_single_active_position_exchange_truth_release = True
    ex.active_symbol_key_prefix = 'orders:active_symbol_sid:'
    ex.active_symbol_guard_tombstone_ttl_sec = 120
    ex.state_key_prefix = 'orders:state:'
    ex.state_ttl = 86400
    ex.exec_journal_primary = False
    ex.exec_state_derived_view = True
    ex.exec_inline_state_projection = True
    ex.execution_journal = None
    return ex


# Test 1: projection worker keeps terminal guard as pending-release when exchange_truth_release=1
def test_projection_worker_keeps_terminal_guard_pending_when_exchange_truth_release_enabled():
    r = FakeRedis()
    worker = worker_mod.ExecutionProjectionWorker(
        r,
        exec_stream='orders:exec',
        state_key_prefix='orders:state:',
        active_symbol_key_prefix='orders:active_symbol_sid:',
        exchange_truth_release=True,
        cursor_key='orders:exec:projection:cursor',
    )
    r.xadd('orders:exec', {
        'sid': 'sid-1',
        'symbol': 'BTCUSDT',
        'action': 'open',
        'event_type': 'state_transition',
        'status': 'ok',
        'fsm_state': 'PROTECTED',
        'ts_event_ms': '1700000000001',
    })
    r.xadd('orders:exec', {
        'sid': 'sid-1',
        'symbol': 'BTCUSDT',
        'action': 'close',
        'event_type': 'state_transition',
        'status': 'closed',
        'fsm_state': 'EXIT_FILLED',
        'ts_event_ms': '1700000000002',
    })
    worker.run_until_idle()
    guard = json.loads(r.get('orders:active_symbol_sid:BTCUSDT'))
    assert guard['sid'] == 'sid-1'
    assert guard['guard_release_policy'] == 'exchange_truth'
    assert guard['guard_release_pending'] is True
    assert guard['state_terminalish'] is True
    assert guard['guard_release_reason'] == 'await_exchange_flat_no_orders'


# Test 2: projection worker still deletes terminal guard in legacy mode (exchange_truth_release=0)
def test_projection_worker_still_deletes_terminal_guard_when_exchange_truth_release_disabled():
    r = FakeRedis()
    worker = worker_mod.ExecutionProjectionWorker(
        r,
        exec_stream='orders:exec',
        state_key_prefix='orders:state:',
        active_symbol_key_prefix='orders:active_symbol_sid:',
        exchange_truth_release=False,
        cursor_key='orders:exec:projection:cursor',
    )
    r.xadd('orders:exec', {
        'sid': 'sid-2',
        'symbol': 'ETHUSDT',
        'action': 'open',
        'event_type': 'state_transition',
        'status': 'ok',
        'fsm_state': 'PROTECTED',
        'ts_event_ms': '1700000000001',
    })
    worker.run_until_idle()
    assert json.loads(r.get('orders:active_symbol_sid:ETHUSDT'))['sid'] == 'sid-2'
    r.xadd('orders:exec', {
        'sid': 'sid-2',
        'symbol': 'ETHUSDT',
        'action': 'close',
        'event_type': 'state_transition',
        'status': 'closed',
        'fsm_state': 'EXIT_FILLED',
        'ts_event_ms': '1700000000002',
    })
    worker.run_until_idle()
    raw_eth = json.loads(r.get('orders:active_symbol_sid:ETHUSDT'))
    assert raw_eth['guard_status'] == 'released'
    assert worker._active_symbol_guard_store().load_active('ETHUSDT') == {}


# Test 3: inline executor and projection worker write identical pending-release contract
def test_inline_executor_and_projection_worker_use_same_pending_release_contract():
    r = FakeRedis()
    ex = _mk_inline_exec(r)
    ex._persist_materialized_state_cache('sid-inline', {
        'sid': 'sid-inline',
        'symbol': 'SOLUSDT',
        'fsm_state': 'EXIT_FILLED',
        'status': 'closed',
        'closed': True,
    })
    inline_guard = json.loads(r.get('orders:active_symbol_sid:SOLUSDT'))
    assert inline_guard['guard_release_policy'] == 'exchange_truth'
    assert inline_guard['guard_release_pending'] is True
    assert inline_guard['state_terminalish'] is True

    r2 = FakeRedis()
    worker = worker_mod.ExecutionProjectionWorker(
        r2,
        exec_stream='orders:exec',
        state_key_prefix='orders:state:',
        active_symbol_key_prefix='orders:active_symbol_sid:',
        exchange_truth_release=True,
        cursor_key='orders:exec:projection:cursor',
    )
    r2.xadd('orders:exec', {
        'sid': 'sid-inline',
        'symbol': 'SOLUSDT',
        'action': 'close',
        'event_type': 'state_transition',
        'status': 'closed',
        'fsm_state': 'EXIT_FILLED',
        'ts_event_ms': '1700000000002',
    })
    worker.run_until_idle()
    projected_guard = json.loads(r2.get('orders:active_symbol_sid:SOLUSDT'))
    assert projected_guard['guard_release_policy'] == inline_guard['guard_release_policy']
    assert projected_guard['guard_release_pending'] == inline_guard['guard_release_pending']
    assert projected_guard['state_terminalish'] == inline_guard['state_terminalish']
    assert projected_guard['guard_release_reason'] == inline_guard['guard_release_reason']


# Test 4: repair worker correctly releases pending-release guard after exchange flat
def test_guard_repair_worker_clears_projection_pending_release_after_exchange_flat():
    r = FakeRedis()
    r.set('orders:active_symbol_sid:BTCUSDT', json.dumps({
        'symbol': 'BTCUSDT',
        'sid': 'sid-flat',
        'guard_release_policy': 'exchange_truth',
        'guard_release_pending': True,
        'state_terminalish': True,
        'guard_release_reason': 'await_exchange_flat_no_orders',
    }))
    worker = repair_mod.BinanceActiveSymbolGuardRepairWorker(redis_client=r, client=FlatClient())
    out = worker.run_once()
    assert out[0]['status'] == 'released'
    raw_btc = json.loads(r.get('orders:active_symbol_sid:BTCUSDT'))
    assert raw_btc['guard_status'] == 'released'
    assert worker._guard_store().load_active('BTCUSDT') == {}
