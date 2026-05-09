from utils.time_utils import get_ny_time_millis

"""P7: High-level integration tests simulating race conditions between workers.

Simulates:
1. Projection worker trying to persist a late event after Repair worker released the guard.
2. Executor trying to open a new trade after Repair worker released the guard.
"""

import importlib.util
import json
import sys
from pathlib import Path

# Load executor
exec_mod_path = Path(__file__).parent.parent / 'binance_executor.py'
exec_spec = importlib.util.spec_from_file_location('binance_executor_integration', exec_mod_path)
exec_mod = importlib.util.module_from_spec(exec_spec)
sys.modules[exec_spec.name] = exec_mod
assert exec_spec.loader is not None
exec_spec.loader.exec_module(exec_mod)

# Load projection worker
proj_mod_path = Path(__file__).parent.parent / 'execution_projection_worker.py'
proj_spec = importlib.util.spec_from_file_location('execution_projection_integration', proj_mod_path)
proj_mod = importlib.util.module_from_spec(proj_spec)
sys.modules[proj_spec.name] = proj_mod
assert proj_spec.loader is not None
proj_spec.loader.exec_module(proj_mod)

# Load repair worker
repair_mod_path = Path(__file__).parent.parent / 'binance_active_symbol_guard_repair_worker.py'
repair_spec = importlib.util.spec_from_file_location('binance_repair_integration', repair_mod_path)
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

    def scan_iter(self, match=None):
        prefix = (match or '').rstrip('*')
        for key in list(self.kv.keys()):
            if not prefix or str(key).startswith(prefix):
                yield key


class FlatClient:
    def get_position_risk(self): return [{"symbol": "ETHUSDT", "positionAmt": "0"}]
    def get_open_orders(self, symbol=None): return []
    def get_open_algo_orders(self, symbol=None): return []


def test_projection_late_same_sid_event_cannot_resurrect_after_repair_release():
    r = FakeRedis()

    # 1. Executor opens the trade initially
    ex = exec_mod.BinanceExecutor.__new__(exec_mod.BinanceExecutor)
    ex.r = r
    ex.exec_single_active_position_per_symbol = True
    ex.exec_single_active_position_exchange_truth_release = True
    ex.active_symbol_key_prefix = 'orders:active_symbol_sid:'
    ex.active_symbol_guard_tombstone_ttl_sec = 120
    ex.state_key_prefix = 'orders:state:'
    ex.state_ttl = 86400

    ex._load_order_state = lambda sid: {}
    ex._guard_single_active_symbol_open(sid='sid-1', symbol='ETHUSDT')
    ex._persist_materialized_state_cache('sid-1', {
        'sid': 'sid-1',
        'symbol': 'ETHUSDT',
        'fsm_state': 'OPEN',
        'status': 'open',
    })

    # 2. Assume trade reached terminal state, but exchange truth flag is true, so projection worker
    # merely sets guard_release_pending=True
    now = get_ny_time_millis()
    guard = json.loads(r.get('orders:active_symbol_sid:ETHUSDT'))
    guard.update({"guard_release_pending": True, "state_terminalish": True, "guard_version": 1})
    r.set('orders:active_symbol_sid:ETHUSDT', json.dumps(guard))

    # 3. Repair worker runs, sees flat exchange, and CAS-releases to tombstone
    repair = repair_mod.BinanceActiveSymbolGuardRepairWorker(redis_client=r, client=FlatClient())
    out = repair.run_once()
    assert out[0]['status'] == 'released'

    guard_post_repair = json.loads(r.get('orders:active_symbol_sid:ETHUSDT'))
    assert guard_post_repair['guard_status'] == 'released'

    # 4. Projection worker wakes up and processes a delayed stream event for sid-1.
    # In P6 this would overwrite the key and resurrect the guard blocking the symbol.
    # In P7 CAS, it must be rejected!
    proj = proj_mod.ExecutionProjectionWorker(
        r, exec_stream='orders:exec', state_key_prefix='orders:state:',
        active_symbol_key_prefix='orders:active_symbol_sid:', cursor_key='orders:cursor'
    )
    r.xadd('orders:exec', {
        'sid': 'sid-1', 'symbol': 'ETHUSDT', 'action': 'cancel', 'fsm_state': 'EXIT_FILLED',
        'event_type': 'state_transition'
    })
    proj.run_until_idle()

    # Verify the key is still a released tombstone
    final_guard = json.loads(r.get('orders:active_symbol_sid:ETHUSDT'))
    assert final_guard['guard_status'] == 'released'


def test_executor_new_sid_can_acquire_after_released_tombstone():
    r = FakeRedis()

    # Setup a tombstone
    from services.active_symbol_guard_store import ActiveSymbolGuardStore
    store = ActiveSymbolGuardStore(r)
    store.acquire_or_refresh(symbol="XRPUSDT", sid="old-sid", payload_patch={}, writer="exec")
    store.mark_released(symbol="XRPUSDT", expected_sid="old-sid", release_reason="test", writer="repair")

    assert json.loads(r.get("orders:active_symbol_sid:XRPUSDT"))['guard_status'] == 'released'

    # Executor attempts new open
    ex = exec_mod.BinanceExecutor.__new__(exec_mod.BinanceExecutor)
    ex.r = r
    ex.exec_single_active_position_per_symbol = True
    ex.exec_single_active_position_exchange_truth_release = True
    ex.active_symbol_key_prefix = 'orders:active_symbol_sid:'
    ex.active_symbol_guard_tombstone_ttl_sec = 120
    ex.state_key_prefix = 'orders:state:'
    ex.state_ttl = 86400
    ex._load_order_state = lambda sid: {}

    # This should succeed and overwrite the tombstone with new sid
    ex._guard_single_active_symbol_open(sid='new-sid', symbol='XRPUSDT')
    ex._persist_materialized_state_cache('new-sid', {
        'sid': 'new-sid',
        'symbol': 'XRPUSDT',
        'fsm_state': 'OPEN',
        'status': 'open',
    })

    final_guard = json.loads(r.get("orders:active_symbol_sid:XRPUSDT"))
    assert final_guard['guard_status'] == 'active'
    assert final_guard['sid'] == 'new-sid'
