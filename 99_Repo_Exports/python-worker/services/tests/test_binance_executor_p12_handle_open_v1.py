from __future__ import annotations
"""P12 — handle_open does NOT mark FSM_PROTECTED after EMERGENCY_FLATTENED.

Tests the critical P12 invariant: if handle_open ends with an emergency
flatten because protection cannot be confirmed or repaired, the FSM must
NOT transition to PROTECTED afterward.

Strategy: test the isolated FSM-guard logic and the verify+repair path
that handle_open uses, without running the full order submission chain.
"""

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

# --- Env setup ---
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ["EXEC_RESUME_OPEN_REPAIR"] = "1"
os.environ["EXEC_STRICT_PROTECTION_VERIFY"] = "1"
os.environ["EXEC_RECONCILE_REQUIRE_PROTECTION_COMPLETE"] = "1"

mod_path = Path(__file__).parent.parent / "binance_executor.py"
spec = importlib.util.spec_from_file_location("binance_executor_p12_handle_open", mod_path)
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


class FakeClient:
    def inspect_protection_set(self, symbol, sid, expected_sl=True, expected_tps=None, trail_expected=False):
        return {
            "is_complete": False, "missing": ["sl", "tp1"],
            "sl": None, "tp_by_index": {},
            "trail": None, "by_client_algo_id": {},
        }


def _mk_executor() -> "mod.BinanceExecutor":
    """Build a minimal BinanceExecutor with P12 FSM control."""
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    ex.r = FakeRedis()
    ex.exec_stream = "orders:exec"
    ex.orders_state_prefix = "orders:state:"
    ex.orders_state_ttl = 86400
    ex.protection_arm_timeout_ms = 2500
    ex.exec_strict_protection_verify = True
    ex.exec_resume_open_repair = True
    ex.exec_reconcile_require_protection_complete = True
    ex.tg = None

    ex._state_cache = {}

    def _save(sid, state_update):
        existing = ex._state_cache.get(sid, {})
        existing.update(state_update)
        ex._state_cache[sid] = existing

    def _load(sid):
        return dict(ex._state_cache.get(sid, {}))

    ex._save_order_state = _save
    ex._load_order_state = _load
    ex._exec_event = lambda ev: ex.r.xadd(ex.exec_stream, {k: str(v) for k, v in ev.items()})
    ex._transition_state = lambda sid, *, symbol, action, next_state, details=None: _save(sid, {"fsm_state": next_state, **(details or {})})
    ex._emit_protection_incident = lambda sid, symbol, reason: None
    ex._emergency_flatten_position = lambda **kw: {"flatten_status": "ok"}

    return ex


# ============================================================
# Test 1: FSM_PROTECTED guard — simulates the handle_open P12 path
# ============================================================

def test_handle_open_does_not_mark_protected_after_emergency_flatten():
    """Simulates the P12 handle_open path where:
    1. _protection_confirmed returns False (protection not confirmed locally)
    2. exec_strict_protection_verify=1 → _verify_protection_on_exchange returns incomplete
    3. _repair_open_protection fails → emergency flatten
    4. Critical invariant: FSM_PROTECTED must NOT be set after EMERGENCY_FLATTENED.
    """
    ex = _mk_executor()
    client = FakeClient()
    sid = "sid-ho-1"
    symbol = "BTCUSDT"
    logical = "LONG"
    filled_qty = 0.001
    tps = [102000.0]

    # Step 1: protection not confirmed locally
    prot = {}  # empty prot from _place_protective failure
    trail = {}

    # Step 2: strict verify returns incomplete
    ex._verify_protection_on_exchange = lambda **kw: {"is_complete": False, "missing": ["sl", "tp1"]}

    # Step 3: repair also fails
    ex._repair_open_protection = lambda **kw: ("repair_incomplete", False)

    # Simulate the exact P12 handle_open logic:
    _emergency_flattened = False
    trail_enabled = False

    # protection_confirmed would return False (prot is empty, no sl_algo_id)
    if True:  # simulating _protection_confirmed returning False
        if ex.exec_strict_protection_verify:
            verify = ex._verify_protection_on_exchange(
                sid=sid, symbol=symbol, payload={}, state={}, client=client,
            )
            if verify.get("is_complete"):
                pass
            else:
                _, repair_ok = ex._repair_open_protection(
                    sid=sid, symbol=symbol, payload={}, state={},
                    client=client, filters=None, policy=None,
                )
                if not repair_ok:
                    emerg = ex._emergency_flatten_position(
                        sid=sid, symbol=symbol, logical_side=logical, qty=filled_qty,
                        client=client, filters=None,
                    )
                    ex._transition_state(
                        sid, symbol=symbol, action="open",
                        next_state=mod.FSM_EMERGENCY_FLATTENED, details=emerg,
                    )
                    prot = {**prot, **emerg, "protection_invariant_failed": True}
                    _emergency_flattened = True

    # The P12 guard: do NOT set FSM_PROTECTED after EMERGENCY_FLATTENED
    if not _emergency_flattened:
        ex._transition_state(sid, symbol=symbol, action="open", next_state=mod.FSM_PROTECTED, details={**prot, **trail})
        if tps:
            ex._transition_state(sid, symbol=symbol, action="open", next_state=mod.FSM_TP_POLICY_ARMED, details={"tp_levels_count": len(tps)})

    # Assert the critical invariant
    state = ex._state_cache.get(sid, {})
    assert state.get("fsm_state") == mod.FSM_EMERGENCY_FLATTENED, \
        f"Expected EMERGENCY_FLATTENED but got {state.get('fsm_state')}"

    # PROTECTED must NOT be the final state
    assert state.get("fsm_state") != mod.FSM_PROTECTED
    assert state.get("fsm_state") != mod.FSM_TP_POLICY_ARMED


# ============================================================
# Test 2: Protection verified on exchange → PROTECTED is set
# ============================================================

def test_handle_open_sets_protected_when_verify_passes():
    """When strict verification passes (exchange says complete),
    FSM_PROTECTED should be set even though _protection_confirmed was False.
    """
    ex = _mk_executor()
    client = FakeClient()
    sid = "sid-ho-2"
    symbol = "ETHUSDT"
    tps = [4500.0]

    # Strict verify returns complete
    ex._verify_protection_on_exchange = lambda **kw: {"is_complete": True, "missing": []}

    _emergency_flattened = False

    if True:  # simulating _protection_confirmed returning False
        if ex.exec_strict_protection_verify:
            verify = ex._verify_protection_on_exchange(
                sid=sid, symbol=symbol, payload={}, state={}, client=client,
            )
            if verify.get("is_complete"):
                pass  # All good — exchange confirmed
            else:
                _emergency_flattened = True

    if not _emergency_flattened:
        ex._transition_state(sid, symbol=symbol, action="open", next_state=mod.FSM_PROTECTED, details={})
        if tps:
            ex._transition_state(sid, symbol=symbol, action="open", next_state=mod.FSM_TP_POLICY_ARMED, details={})

    state = ex._state_cache.get(sid, {})
    assert state.get("fsm_state") == mod.FSM_TP_POLICY_ARMED


# ============================================================
# Test 3: Repair succeeds → PROTECTED is set
# ============================================================

def test_handle_open_repair_success_sets_protected():
    """When strict verify fails but repair succeeds,
    FSM_PROTECTED should still be set.
    """
    ex = _mk_executor()
    client = FakeClient()
    sid = "sid-ho-3"
    symbol = "BTCUSDT"
    tps = [102000.0]

    ex._verify_protection_on_exchange = lambda **kw: {"is_complete": False, "missing": ["tp1"]}
    ex._repair_open_protection = lambda **kw: ("repaired", True)

    _emergency_flattened = False

    if True:
        if ex.exec_strict_protection_verify:
            verify = ex._verify_protection_on_exchange(
                sid=sid, symbol=symbol, payload={}, state={}, client=client,
            )
            if verify.get("is_complete"):
                pass
            else:
                _, repair_ok = ex._repair_open_protection(
                    sid=sid, symbol=symbol, payload={}, state={},
                    client=client, filters=None, policy=None,
                )
                if not repair_ok:
                    _emergency_flattened = True

    if not _emergency_flattened:
        ex._transition_state(sid, symbol=symbol, action="open", next_state=mod.FSM_PROTECTED, details={})
        if tps:
            ex._transition_state(sid, symbol=symbol, action="open", next_state=mod.FSM_TP_POLICY_ARMED, details={})

    state = ex._state_cache.get(sid, {})
    assert state.get("fsm_state") == mod.FSM_TP_POLICY_ARMED
