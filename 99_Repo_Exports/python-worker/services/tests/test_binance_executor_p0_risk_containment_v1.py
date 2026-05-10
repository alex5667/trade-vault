from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from core.redis_keys import RedisStreams as RS

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import pytest

mod_path = Path(__file__).parent.parent / 'binance_executor.py'
spec = importlib.util.spec_from_file_location('binance_executor_p0_risk_containment', mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


class FakeRedis:
    def __init__(self):
        self.stream = []

    def xadd(self, key, fields, maxlen=None, approximate=True):
        self.stream.append((key, dict(fields)))


def _mk_exec():
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    ex.exec_disable_modify_on_binance = True
    ex.exec_disable_resize_on_binance = True
    ex.exec_blocked_action_reason = 'operator_risk_hold'
    ex.exec_blocked_action_state_write = True
    ex.r = FakeRedis()
    ex.exec_stream = RS.ORDERS_EXEC
    ex.allowlist = set()
    ex.saved = []
    ex.acked = []
    ex._save_order_state = lambda sid, state: ex.saved.append((sid, dict(state)))
    ex._exec_event = lambda event: ex.r.xadd(ex.exec_stream, event)
    ex._ack_processing = lambda raw: ex.acked.append(raw)
    return ex


def test_handle_modify_blocked_by_feature_flag_before_client_resolution():
    ex = _mk_exec()
    with pytest.raises(mod.ExecutionActionBlockedError) as ei:
        ex.handle_modify({'sid': 'sid-1', 'symbol': 'BTCUSDT'})
    assert ei.value.details['action'] == 'modify'
    assert ei.value.details['status'] == 'blocked'
    assert ex.saved[0][1]['last_blocked_action'] == 'modify'


def test_handle_resize_blocked_by_feature_flag_before_client_resolution():
    ex = _mk_exec()
    with pytest.raises(mod.ExecutionActionBlockedError) as ei:
        ex.handle_resize({'sid': 'sid-2', 'symbol': 'ETHUSDT'})
    assert ei.value.details['action'] == 'resize'
    assert ex.saved[0][1]['last_blocked_action'] == 'resize'


def test_process_one_acknowledges_blocked_modify_without_dlq():
    ex = _mk_exec()
    raw = json.dumps({'action': 'modify', 'sid': 'sid-3', 'symbol': 'BTCUSDT'})
    ex.process_one(raw)
    assert ex.acked == [raw]
    assert ex.r.stream
    key, event = ex.r.stream[-1]
    assert key == RS.ORDERS_EXEC
    assert event['status'] == 'blocked'
    assert event['action'] == 'modify'
