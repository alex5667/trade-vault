from pathlib import Path
import importlib.util
import json
import sys

exec_mod_path = Path(__file__).parent.parent / 'binance_executor.py'
exec_spec = importlib.util.spec_from_file_location('binance_executor_p121', exec_mod_path)
exec_mod = importlib.util.module_from_spec(exec_spec)
sys.modules[exec_spec.name] = exec_mod
assert exec_spec.loader is not None
exec_spec.loader.exec_module(exec_mod)

worker_mod_path = Path(__file__).parent.parent / 'execution_projection_worker.py'
worker_spec = importlib.util.spec_from_file_location('execution_projection_worker_p121', worker_mod_path)
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

    def xadd(self, key, fields, maxlen=None, approximate=None):
        self._seq += 1
        sid = f"{1700000000000 + self._seq}-0"
        self.streams.setdefault(key, []).append((sid, dict(fields)))
        return sid

    def xrange(self, key, start='-', end='+', count=None):
        rows = list(self.streams.get(key, []))
        if count is not None:
            rows = rows[: int(count)]
        return rows

    def xrevrange(self, key, start='+', end='-', count=None):
        return list(reversed(self.xrange(key)))


def _mk_exec(redis_obj, *, inline_projection=False):
    ex = exec_mod.BinanceExecutor.__new__(exec_mod.BinanceExecutor)
    ex.r = redis_obj
    ex.exec_stream = 'orders:exec'
    ex.state_key_prefix = 'orders:state:'
    ex.state_ttl = 86400
    ex.exec_rehydrate_on_state_miss = True
    ex.exec_replay_scan_count = 500
    ex.exec_replay_checkpoint_key_prefix = 'orders:exec:replay:cursor:'
    ex.exec_replay_quarantine_on_mismatch = False
    ex.exec_journal_primary = True
    ex.exec_state_derived_view = True
    ex.exec_inline_state_projection = inline_projection
    ex.execution_journal = None
    return ex


def test_executor_does_not_materialize_cache_inline_when_projection_worker_is_enabled():
    """When EXEC_INLINE_STATE_PROJECTION=0, executor must NOT write orders:state:{sid}."""
    r = FakeRedis()
    ex = _mk_exec(r, inline_projection=False)
    sid = 'sid-inline-off'
    ex._transition_state(
        sid
        symbol='BTCUSDT'
        action='open'
        next_state='ENTRY_ACKED'
        details={'entry_client_order_id': 'cid-1', 'binance_order_id': 111}
    )
    # With inline_projection=False, no cache write should happen
    assert r.get(f'orders:state:{sid}') in (None, '')

    # Projection worker materialises state from stream
    worker = worker_mod.ExecutionProjectionWorker(r, exec_stream='orders:exec', state_key_prefix='orders:state:')
    processed = worker.run_until_idle()
    assert processed >= 1
    state = json.loads(r.get(f'orders:state:{sid}'))
    assert state['fsm_state'] == 'ENTRY_ACKED'
    assert int(state['entry']['order_id']) == 111
    assert state['entry']['client_order_id'] == 'cid-1'


def test_save_order_state_emits_state_patch_and_worker_applies_it_in_order():
    """state_patch events must be applied by the deferred projection worker."""
    r = FakeRedis()
    ex = _mk_exec(r, inline_projection=False)
    sid = 'sid-patch'
    ex._exec_event({
        'sid': sid
        'symbol': 'ETHUSDT'
        'action': 'open'
        'event_type': 'state_transition'
        'status': 'ok'
        'fsm_state': 'PROTECTED'
        'binance_order_id': 501
        'tp1_algo_id': 601
        'tp1_client_algo_id': 'tp1-a'
    })
    ex._save_order_state(sid, {'trail_algo_id': 701, 'trail_client_algo_id': 'trail-a', 'symbol': 'ETHUSDT'})
    # No inline cache write
    assert r.get(f'orders:state:{sid}') in (None, '')
    assert [row[1]['event_type'] for row in r.streams['orders:exec']] == ['state_transition', 'state_patch']

    worker = worker_mod.ExecutionProjectionWorker(r, exec_stream='orders:exec', state_key_prefix='orders:state:')
    worker.run_until_idle()
    state = json.loads(r.get(f'orders:state:{sid}'))
    assert state['fsm_state'] == 'PROTECTED'
    assert state['protective']['tp_algo_ids'] == [601]
    assert int(state['trailing']['algo_id']) == 701


def test_projection_worker_cursor_makes_repeated_runs_idempotent():
    """Running projection worker twice must not double-process events."""
    r = FakeRedis()
    ex = _mk_exec(r, inline_projection=False)
    sid = 'sid-cursor'
    ex._exec_event({
        'sid': sid
        'symbol': 'SOLUSDT'
        'action': 'open'
        'event_type': 'state_transition'
        'status': 'ok'
        'fsm_state': 'ENTRY_FILLED'
        'binance_order_id': 901
    })
    worker = worker_mod.ExecutionProjectionWorker(
        r
        exec_stream='orders:exec'
        state_key_prefix='orders:state:'
        cursor_key='orders:exec:projection:cursor'
    )
    first = worker.run_once()
    second = worker.run_once()
    state = json.loads(r.get(f'orders:state:{sid}'))
    assert first.processed == 1
    assert second.processed == 0
    assert state['fsm_state'] == 'ENTRY_FILLED'
    assert r.get('orders:exec:projection:cursor') == first.last_stream_id
