"""P12 — Strict protection verification & repair tests.

Tests:
1. _resume_open_from_state  — repaired missing TP → FSM_PROTECTED + FSM_TP_POLICY_ARMED
2. _attempt_reconcile_after_exception — partial protection → empty dict (blocked)
3. _attempt_reconcile_after_exception — protection complete → resolved dict
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

# --- Env setup before module import ---
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ["EXEC_RESUME_OPEN_REPAIR"] = "1"
os.environ["EXEC_STRICT_PROTECTION_VERIFY"] = "1"
os.environ["EXEC_RECONCILE_REQUIRE_PROTECTION_COMPLETE"] = "1"

mod_path = Path(__file__).parent.parent / "binance_executor.py"
spec = importlib.util.spec_from_file_location("binance_executor_p12_strict", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


# --- Fakes ---

class FakeRedis:
    def __init__(self):
        self.store: Dict[str, str] = {}
        self.stream: list = []

    def get(self, key: str) -> Optional[bytes]:
        v = self.store.get(key)
        return v.encode() if v else None

    def set(self, key: str, value: str, ex: int = None) -> None:
        self.store[key] = value

    def xadd(self, key: str, fields: dict, maxlen: int = None, approximate: bool = True) -> str:
        self.stream.append((key, dict(fields)))
        return "0-1"

    def sismember(self, key: str, member: str) -> bool:
        return False


class FakeClient:
    """Mock BinanceFuturesClient with inspect_protection_set control."""

    def __init__(self, *, inspect_result: dict = None, post_result: dict = None):
        self._inspect_result = inspect_result or {
            "is_complete": True, "missing": [],
            "sl": {"algoId": 1}, "tp_by_index": {1: {"algoId": 2}},
            "trail": None, "by_client_algo_id": {},
        }
        self._post_result = post_result or {}
        self.calls: List[tuple] = []

    def inspect_protection_set(self, symbol, sid, expected_sl=True, expected_tps=None, trail_expected=False):
        self.calls.append(("inspect_protection_set", symbol, sid))
        return dict(self._inspect_result)

    def reconcile_protection_by_sid(self, symbol, sid):
        return self.inspect_protection_set(symbol, sid)

    def reconcile_entry_by_client_id(self, symbol, client_order_id):
        return {"orderId": 12345, "status": "FILLED"}

    def query_plain_order(self, symbol, order_id=None, client_order_id=None):
        return {"orderId": order_id or 12345, "status": "FILLED"}

    def get_open_algo_orders(self, symbol):
        return []

    def post_algo_order(self, params):
        return {"algoId": 999}


def _mk_executor(**env_overrides) -> "mod.BinanceExecutor":
    """Build a BinanceExecutor stub with minimal wiring."""
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    ex.r = FakeRedis()
    ex.exec_stream = "orders:exec"
    ex.orders_state_prefix = "orders:state:"
    ex.orders_state_ttl = 86400
    ex.allowlist = {"BTCUSDT", "ETHUSDT"}
    ex.position_mode = "oneway"
    ex.protection_arm_timeout_ms = 2500
    ex.exec_resume_open_repair = True
    ex.exec_strict_protection_verify = True
    ex.exec_reconcile_require_protection_complete = True
    ex.fill_timeout_s = 10.0
    ex.tg = None

    # --- State management ---
    ex._state_cache = {}

    def _save(sid, state_update):
        existing = ex._state_cache.get(sid, {})
        existing.update(state_update)
        ex._state_cache[sid] = existing
        ex.r.set(f"orders:state:{sid}", json.dumps(existing))

    def _load(sid):
        return dict(ex._state_cache.get(sid, {}))

    ex._save_order_state = _save
    ex._load_order_state = _load

    # --- Minimal stubs ---
    ex._exec_event = lambda ev: ex.r.xadd(ex.exec_stream, {k: str(v) for k, v in ev.items()})
    ex._transition_state = lambda sid, *, symbol, action, next_state, details=None: _save(sid, {"fsm_state": next_state, **(details or {})})
    ex._resolve_client = lambda payload: (FakeClient(), None)
    ex._resolve_execution_policy = lambda payload, symbol: MagicMock(name="SAFETY_FIRST")
    ex._emit_protection_incident = lambda sid, symbol, reason: None
    ex._emergency_flatten_position = lambda **kw: {"flatten_status": "ok"}
    ex._start_lifecycle_watchdog = lambda *a, **kw: None

    # Override protection placement for tests
    ex._place_protective = lambda **kw: {"sl_algo_id": 100, "tp1_algo_id": 200}

    # Apply overrides
    for k, v in env_overrides.items():
        setattr(ex, k, v)

    return ex


# ============================================================
# Test 1: _resume_open_from_state repairs missing TP → PROTECTED
# ============================================================

def test_resume_open_repairs_missing_tp_and_transitions_protected():
    """When resuming from ENTRY_FILLED with exec_resume_open_repair=1,
    verify protection on exchange; if incomplete, repair; then transition
    to FSM_PROTECTED + FSM_TP_POLICY_ARMED.
    """
    ex = _mk_executor()

    # Pre-seed state: entry filled but not protected
    ex._state_cache["sid-repair-1"] = {
        "symbol": "BTCUSDT",
        "fsm_state": mod.FSM_ENTRY_FILLED,
        "side": "LONG",
        "qty": 0.001,
        "exec_price": 100000.0,
        "sl_requested": 99000.0,
        "tp_levels_requested": [102000.0],
        "trail_after_tp1_requested": False,
    }

    # inspect_protection_set: first call returns incomplete, second (after repair) returns complete
    call_count = {"n": 0}
    def mock_inspect(*, sid, symbol, payload, state, client):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return {"is_complete": False, "missing": ["tp1"]}
        return {"is_complete": True, "missing": []}

    ex._verify_protection_on_exchange = mock_inspect
    ex._repair_open_protection = lambda **kw: ("repaired", True)

    result = ex._resume_open_from_state("sid-repair-1", symbol="BTCUSDT", client=FakeClient())

    assert result is not None
    assert result.get("recovered_from_state") is True
    assert result.get("resume_repair") == "repaired"
    state = ex._state_cache["sid-repair-1"]
    assert state["fsm_state"] == mod.FSM_TP_POLICY_ARMED


# ============================================================
# Test 2: _attempt_reconcile requires complete protection for open
# ============================================================

def test_reconcile_requires_complete_protection_for_open():
    """When exec_reconcile_require_protection_complete=1 and action='open',
    partial protection (is_complete=False) should return empty dict.
    """
    ex = _mk_executor()

    # Mock _reconcile_entry_by_client_id to return a found entry
    ex._reconcile_entry_by_client_id = lambda **kw: {"orderId": 111, "status": "FILLED"}

    # Mock _reconcile_protection_by_sid to return incomplete protection
    ex._reconcile_protection_by_sid = lambda **kw: {
        "is_complete": False, "missing": ["sl"],
        "sl": None, "tp_by_index": {1: {"algoId": 2}},
    }

    # Mock repair to also fail
    ex._repair_open_protection = lambda **kw: ("repair_incomplete", False)

    result = ex._attempt_reconcile_after_exception(
        payload={"sid": "sid-recon-1", "symbol": "BTCUSDT"},
        action="open",
        symbol="BTCUSDT",
        client=FakeClient(),
    )

    assert result == {}


# ============================================================
# Test 3: _attempt_reconcile returns resolved when protection complete
# ============================================================

def test_reconcile_returns_resolved_when_protection_complete():
    """When protection is complete, reconcile returns resolved dict."""
    ex = _mk_executor()

    ex._reconcile_entry_by_client_id = lambda **kw: {"orderId": 222, "status": "FILLED"}
    ex._reconcile_protection_by_sid = lambda **kw: {
        "is_complete": True, "missing": [],
        "sl": {"algoId": 10}, "tp_by_index": {1: {"algoId": 20}},
    }

    result = ex._attempt_reconcile_after_exception(
        payload={"sid": "sid-recon-2", "symbol": "ETHUSDT"},
        action="open",
        symbol="ETHUSDT",
        client=FakeClient(),
    )

    assert result.get("event_type") == "reconcile_resolved"
    assert result.get("protection_complete") is True
    assert result.get("reconciled_entry_order_id") == 222


# ============================================================
# Test 4: _resume_open emergency flatten when repair fails
# ============================================================

def test_resume_open_emergency_flatten_when_repair_fails():
    """If repair fails during resume, position is emergency-flattened."""
    ex = _mk_executor()

    ex._state_cache["sid-fail-1"] = {
        "symbol": "BTCUSDT",
        "fsm_state": mod.FSM_ENTRY_FILLED,
        "side": "LONG",
        "qty": 0.001,
        "exec_price": 100000.0,
        "sl_requested": 99000.0,
        "tp_levels_requested": [102000.0],
    }

    # Verify returns incomplete, repair also fails
    ex._verify_protection_on_exchange = lambda **kw: {"is_complete": False, "missing": ["sl", "tp1"]}
    ex._repair_open_protection = lambda **kw: (_ for _ in ()).throw(RuntimeError("exchange_down"))

    result = ex._resume_open_from_state("sid-fail-1", symbol="BTCUSDT", client=FakeClient())

    assert result is not None
    assert result.get("resume_repair") == "emergency_flattened"
    assert ex._state_cache["sid-fail-1"]["fsm_state"] == mod.FSM_EMERGENCY_FLATTENED


# ============================================================
# Test 5: verify_protection_on_exchange returns complete when SL+TP present
# ============================================================

def test_verify_protection_complete():
    """_verify_protection_on_exchange returns is_complete=True when SL+TP are on exchange."""
    ex = _mk_executor()

    client = FakeClient(inspect_result={
        "is_complete": True, "missing": [],
        "sl": {"algoId": 1}, "tp_by_index": {1: {"algoId": 2}},
        "trail": None, "by_client_algo_id": {},
    })

    result = ex._verify_protection_on_exchange(
        sid="sid-v-1", symbol="BTCUSDT",
        payload={"sl": 99000.0, "tp_levels": [102000.0]},
        state={}, client=client,
    )

    assert result["is_complete"] is True
    assert result["missing"] == []
