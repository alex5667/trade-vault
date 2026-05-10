import importlib.util
import json
import sys
from pathlib import Path
from core.redis_keys import RedisStreams as RS

mod_path = Path(__file__).parent.parent.parent / 'services' / 'binance_executor.py'
spec = importlib.util.spec_from_file_location('binance_executor_p112', mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.streams = {}
        self._seq = 0

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None):
        self.kv[key] = value

    def scan_iter(self, match=None):
        return iter([])

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
        rows = list(reversed(self.streams.get(key, [])))
        if start not in ('+', None):
            rows = [r for r in rows if r[0] <= str(start)]
        if end not in ('-', None):
            rows = [r for r in rows if r[0] >= str(end)]
        if count is not None:
            rows = rows[: int(count)]
        return rows



def _mk_exec():
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    ex.r = FakeRedis()
    ex.exec_stream = RS.ORDERS_EXEC
    ex.state_key_prefix = 'orders:state:'
    ex.state_ttl = 86400
    ex.exec_rehydrate_on_state_miss = True
    ex.exec_replay_scan_count = 500
    ex.exec_replay_checkpoint_key_prefix = 'orders:exec:replay:cursor:'
    ex.exec_replay_quarantine_on_mismatch = False
    ex.exec_journal_primary = True
    ex.exec_state_derived_view = True
    ex.execution_journal = None
    return ex


def test_load_order_state_prefers_exec_stream_over_stale_cache():
    ex = _mk_exec()
    sid = 'sid-1'
    stale = {
        'sid': sid,
        'symbol': 'BTCUSDT',
        'action': 'open',
        'fsm_state': 'FAILED',
        'status': 'failed',
    }
    ex.r.set(f'orders:state:{sid}', json.dumps(stale))
    ex._exec_event({
        'sid': sid,
        'symbol': 'BTCUSDT',
        'action': 'open',
        'event_type': 'state_transition',
        'status': 'ok',
        'fsm_state': 'PROTECTED',
        'prev_state': 'ENTRY_FILLED',
        'binance_order_id': 101,
        'sl_algo_id': 202,
    })
    state = ex._load_order_state(sid)
    assert state['fsm_state'] == 'PROTECTED'
    assert int(state['protective']['sl_algo_id']) == 202
    assert int(state['entry']['order_id']) == 101


def test_transition_state_is_journal_first_and_updates_cache():
    ex = _mk_exec()
    sid = 'sid-2'
    out = ex._transition_state(
        sid,
        symbol='ETHUSDT',
        action='open',
        next_state='ENTRY_ACKED',
        details={'entry_client_order_id': 'cid-1', 'binance_order_id': 777},
    )
    assert out['fsm_state'] == 'ENTRY_ACKED'
    cache = json.loads(ex.r.get(f'orders:state:{sid}'))
    assert cache['fsm_state'] == 'ENTRY_ACKED'
    assert cache['entry']['client_order_id'] == 'cid-1'
    assert ex.r.streams[RS.ORDERS_EXEC][-1][1]['event_type'] == 'state_transition'


def test_save_order_state_merges_patch_on_top_of_journal_state():
    ex = _mk_exec()
    sid = 'sid-3'
    ex._exec_event({
        'sid': sid,
        'symbol': 'SOLUSDT',
        'action': 'open',
        'event_type': 'state_transition',
        'status': 'ok',
        'fsm_state': 'PROTECTED',
        'binance_order_id': 501,
        'tp1_algo_id': 601,
        'tp1_client_algo_id': 'tp1-a',
    })
    ex._save_order_state(sid, {'trail_algo_id': 701, 'trail_client_algo_id': 'trail-a'})
    state = json.loads(ex.r.get(f'orders:state:{sid}'))
    assert state['fsm_state'] == 'PROTECTED'
    assert int(state['entry']['order_id']) == 501
    assert int(state['trailing']['algo_id']) == 701
    assert state['protective']['tp_algo_ids'] == [601]
