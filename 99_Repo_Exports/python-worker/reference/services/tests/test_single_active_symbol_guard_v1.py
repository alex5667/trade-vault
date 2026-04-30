from pathlib import Path
import importlib.util
import json
import sys

exec_mod_path = Path(__file__).parent.parent / 'binance_executor.py'
exec_spec = importlib.util.spec_from_file_location('binance_executor_single_active', exec_mod_path)
exec_mod = importlib.util.module_from_spec(exec_spec)
sys.modules[exec_spec.name] = exec_mod
assert exec_spec.loader is not None
exec_spec.loader.exec_module(exec_mod)

worker_mod_path = Path(__file__).parent.parent / 'execution_projection_worker.py'
worker_spec = importlib.util.spec_from_file_location('execution_projection_worker_single_active', worker_mod_path)
worker_mod = importlib.util.module_from_spec(worker_spec)
sys.modules[worker_spec.name] = worker_mod
assert worker_spec.loader is not None
worker_spec.loader.exec_module(worker_mod)


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
            rows = [row for row in rows if row[0] >= start]
        if count is not None:
            rows = rows[: int(count)]
        return rows

    def scan_iter(self, match=None):
        prefix = str(match or '').rstrip('*')
        for key in list(self.kv.keys()):
            if not prefix or str(key).startswith(prefix):
                yield key


def _mk_exec(redis_obj):
    ex = exec_mod.BinanceExecutor.__new__(exec_mod.BinanceExecutor)
    ex.r = redis_obj
    ex.exec_single_active_position_per_symbol = True
    ex.exec_single_active_position_release_on_terminal = True
    ex.exec_single_active_position_stale_timeout_ms = 900000
    ex.active_symbol_key_prefix = 'orders:active_symbol_sid:'
    ex.exec_journal_primary = True
    ex.exec_state_derived_view = True
    ex.exec_inline_state_projection = False
    ex.state_key_prefix = 'orders:state:'
    ex.state_ttl = 86400
    return ex


def test_projection_worker_sets_and_clears_active_symbol_key_from_journal():
    r = FakeRedis()
    worker = worker_mod.ExecutionProjectionWorker(
        r
        exec_stream='orders:exec'
        state_key_prefix='orders:state:'
        active_symbol_key_prefix='orders:active_symbol_sid:'
        cursor_key='orders:exec:projection:cursor'
    )
    r.xadd('orders:exec', {
        'sid': 'sid-1'
        'symbol': 'BTCUSDT'
        'action': 'open'
        'event_type': 'state_transition'
        'status': 'ok'
        'fsm_state': 'PROTECTED'
        'ts_event_ms': '1700000000001'
    })
    worker.run_until_idle()
    active = json.loads(r.get('orders:active_symbol_sid:BTCUSDT'))
    assert active['sid'] == 'sid-1'
    assert active['fsm_state'] == 'PROTECTED'

    r.xadd('orders:exec', {
        'sid': 'sid-1'
        'symbol': 'BTCUSDT'
        'action': 'cancel'
        'event_type': 'state_transition'
        'status': 'closed'
        'fsm_state': 'EXIT_FILLED'
        'ts_event_ms': '1700000000002'
    })
    worker.run_until_idle()
    assert r.get('orders:active_symbol_sid:BTCUSDT') in (None, '')


def test_executor_blocks_new_open_when_symbol_has_active_sid():
    r = FakeRedis()
    r.set('orders:active_symbol_sid:ETHUSDT', json.dumps({
        'symbol': 'ETHUSDT'
        'sid': 'sid-existing'
        'fsm_state': 'PROTECTED'
        'updated_at_ms': 1700000000000
    }))
    ex = _mk_exec(r)
    ex._load_order_state = lambda sid: {'sid': sid, 'fsm_state': 'PROTECTED'}
    try:
        ex._guard_single_active_symbol_open(sid='sid-new', symbol='ETHUSDT')
        assert False, 'expected OpenBlockedByActiveSymbolError'
    except exec_mod.OpenBlockedByActiveSymbolError as e:
        assert e.details['blocked_by_sid'] == 'sid-existing'
        assert e.details['blocked_by_state'] == 'PROTECTED'


def test_executor_releases_terminal_active_symbol_guard_before_new_open():
    r = FakeRedis()
    r.set('orders:active_symbol_sid:SOLUSDT', json.dumps({
        'symbol': 'SOLUSDT'
        'sid': 'sid-old'
        'fsm_state': 'PROTECTED'
        'updated_at_ms': 1700000000000
    }))
    ex = _mk_exec(r)
    ex._load_order_state = lambda sid: {'sid': sid, 'fsm_state': 'EXIT_FILLED', 'status': 'closed', 'closed': True}
    ex._guard_single_active_symbol_open(sid='sid-new', symbol='SOLUSDT')
    assert r.get('orders:active_symbol_sid:SOLUSDT') in (None, '')
