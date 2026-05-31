"""P3 — Strict modify/resize protection-replace invariant tests.

Tests:
1. _replace_position_protection → ok path: FSM_PROTECTED transition + metrics
2. _replace_position_protection → verify fail → EMERGENCY_FLATTENED
3. handle_modify uses state-saved sl/tp when payload omits them
4. handle_resize returns {"status": "flat"} when position resolves to zero after resize
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
os.environ["EXEC_MODIFY_RESIZE_STRICT_REPLACE"] = "1"
os.environ["PROTECTION_REPLACE_MAX_NAKED_MS"] = "3000"

mod_path = Path(__file__).parent.parent / "binance_executor.py"
spec = importlib.util.spec_from_file_location("binance_executor_p3_ri", mod_path)
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
    """Mock BinanceFuturesClient for P3 tests.

    Supports configurable inspect_protection_set results, position_amt, and
    tracking of cancel / post_algo_order / cancel_all_orders calls.
    """

    def __init__(
        self,
        *,
        position_amt: float = 0.01,
        inspect_complete: bool = True,
        inspect_missing: Optional[List[str]] = None,
        inspect_mismatched: Optional[List[str]] = None,
    ):
        self._position_amt = position_amt
        self._inspect_complete = inspect_complete
        self._inspect_missing = inspect_missing or []
        self._inspect_mismatched = inspect_mismatched or []
        self.calls: List[tuple] = []

    @staticmethod
    def _build_client_algo_id(sid: str, tag: str) -> str:
        import hashlib
        token = hashlib.sha1(str(sid).encode()).hexdigest()[:8]
        base = str(sid).replace(" ", "").replace(":", "-")
        base = base[: max(6, 36 - (len(tag) + len(token) + 2))]
        return f"{base}-{token}-{tag}"[:36]

    def get_position_risk(self) -> List[Dict[str, Any]]:
        self.calls.append(("get_position_risk",))
        return [
            {"symbol": "BTCUSDT", "positionAmt": str(self._position_amt)}
        ]

    def cancel_algo_order(self, symbol: str, **kwargs) -> Dict[str, Any]:
        self.calls.append(("cancel_algo_order", symbol, kwargs))
        return {"status": "CANCELED"}

    def cancel_all_orders(self, symbol: str) -> Any:
        self.calls.append(("cancel_all_orders", symbol))
        return {}

    def post_algo_order(self, params: dict) -> dict:
        self.calls.append(("post_algo_order", params))
        return {"algoId": 9001}

    def get_mark_price(self, symbol: str) -> float:
        return 100_000.0

    def inspect_protection_set(
        self,
        symbol: str,
        sid: str,
        expected_sl: bool = True,
        expected_tps: Optional[List[float]] = None,
        trail_expected: bool = False,
        expected_sl_price: Optional[float] = None,
        expected_tp_prices: Optional[List[float]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        self.calls.append(("inspect_protection_set", symbol, sid))
        return {
            "is_complete": self._inspect_complete,
            "missing": list(self._inspect_missing),
            "mismatched": list(self._inspect_mismatched),
            "sl": {"algoId": 1} if self._inspect_complete else None,
            "tp_by_index": {1: {"algoId": 2}} if self._inspect_complete else {},
            "trail": None,
            "by_client_algo_id": {},
        }


def _mk_executor(*, position_amt: float = 0.01, inspect_complete: bool = True, **overrides):
    """Build a minimal BinanceExecutor stub for P3 tests."""
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    ex.r = FakeRedis()
    ex.exec_stream = "orders:exec"
    ex.orders_state_prefix = "orders:state:"
    ex.orders_state_ttl = 86400
    ex.allowlist = {"BTCUSDT", "ETHUSDT"}
    ex.position_mode = "oneway"
    ex.protection_arm_timeout_ms = 2500
    ex.protection_replace_max_naked_ms = 3000
    ex.exec_resume_open_repair = True
    ex.exec_strict_protection_verify = True
    ex.exec_modify_resize_strict_replace = True
    ex.exec_reconcile_require_protection_complete = True
    ex.rollout_flags = MagicMock()
    ex.rollout_flags.exec_maker_tp_enable = False
    ex.exec_policy_default = "SAFETY_FIRST"
    ex.exec_policy_maker_allowed_symbols = {"BTCUSDT", "ETHUSDT"}
    ex.fill_timeout_s = 10.0
    ex.tg = None

    # State management
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

    fake_client = FakeClient(position_amt=position_amt, inspect_complete=inspect_complete)

    # Minimal stubs
    ex._exec_event = lambda ev: ex.r.xadd(ex.exec_stream, {k: str(v) for k, v in ev.items()})
    ex._transition_state = lambda sid, *, symbol, action, next_state, details=None: _save(sid, {"fsm_state": next_state, **(details or {})})
    ex._resolve_client = lambda payload: (fake_client, MagicMock())
    class _FakePolicy:
        name = "SAFETY_FIRST"
        tp_working_type = "MARK_PRICE"

    ex._resolve_execution_policy = lambda payload, symbol=None: _FakePolicy()
    ex._emit_protection_incident = lambda sid, symbol, reason: None
    ex._emergency_flatten_position = lambda **kw: {"flatten_status": "ok", "flat_side": kw.get("logical_side")}
    ex._start_lifecycle_watchdog = lambda *a, **kw: None
    ex._guard_binance_action_enabled = lambda *, action, sid, symbol: None
    ex._guard_sid_not_quarantined = lambda sid, *, symbol, action: None
    ex._sync_client_clock = lambda client: None
    ex._place_protective = lambda **kw: {"sl_algo_id": 100, "tp1_algo_id": 200, "tp1_working_type": "MARK_PRICE"}
    ex._maybe_start_trailing_after_tp1 = lambda **kw: {}
    ex._cancel_by_token = lambda symbol, sid, *, client=None: []
    ex._derive_audit_chain_fields = lambda payload, sid: {}
    ex._derive_entry_exit_policies = lambda *, execution_policy: {}

    for k, v in overrides.items():
        setattr(ex, k, v)

    # Client reference for assertions
    ex._fake_client = fake_client
    return ex


# =============================================================================
# Test 1: _replace_position_protection — ok path
# =============================================================================

def test_replace_position_protection_transitions_protected():
    """Success path: protection placed and verified → FSM_PROTECTED + result status=ok."""
    ex = _mk_executor(inspect_complete=True)

    result = ex._replace_position_protection(
        sid="sid-pp-1",
        symbol="BTCUSDT",
        action="modify",
        logical_side="LONG",
        live_qty=0.001,
        sl=98000.0,
        tps=[102000.0],
        payload={"trail_after_tp1_requested": False},
        policy=ex._resolve_execution_policy({}),
        client=ex._fake_client,
        filters=MagicMock(),
        ref_price=100_000.0,
    )

    assert result.get("status") == "ok", f"Expected ok, got: {result}"
    state = ex._state_cache.get("sid-pp-1", {})
    # When tps are provided, final FSM state is FSM_TP_POLICY_ARMED (after FSM_PROTECTED)
    assert state.get("fsm_state") in {mod.FSM_PROTECTED, mod.FSM_TP_POLICY_ARMED}, (
        f"Unexpected FSM state: {state.get('fsm_state')}"
    )


# =============================================================================
# Test 2: _replace_position_protection — verify fail → emergency flatten
# =============================================================================

def test_replace_position_protection_emergency_on_verify_fail():
    """When verify returns is_complete=False, emergency flatten is triggered."""
    ex = _mk_executor(inspect_complete=False)
    ex._inspect_missing = ["sl"]

    result = ex._replace_position_protection(
        sid="sid-pp-2",
        symbol="BTCUSDT",
        action="modify",
        logical_side="LONG",
        live_qty=0.001,
        sl=98000.0,
        tps=[102000.0],
        payload={"trail_after_tp1_requested": False},
        policy=ex._resolve_execution_policy({}),
        client=ex._fake_client,
        filters=MagicMock(),
        ref_price=100_000.0,
    )

    assert result.get("status") == "emergency_flattened", f"Expected emergency_flattened, got: {result}"
    state = ex._state_cache.get("sid-pp-2", {})
    assert state.get("fsm_state") == mod.FSM_EMERGENCY_FLATTENED, f"FSM not FSM_EMERGENCY_FLATTENED: {state.get('fsm_state')}"


# =============================================================================
# Test 3: handle_modify falls back to state sl/tp when payload omits them
# =============================================================================

def test_handle_modify_uses_state_protection_when_payload_omits_levels():
    """When payload has no sl/tp but state has sl_requested + tp_levels_requested,
    _replace_position_protection is called with the state-saved values.
    """
    ex = _mk_executor(position_amt=0.001, inspect_complete=True)

    # Seed state with a previous open contract
    ex._state_cache["sid-modify-1"] = {
        "symbol": "BTCUSDT",
        "fsm_state": mod.FSM_PROTECTED,
        "side": "LONG",
        "qty": 0.001,
        "sl_requested": 98000.0,
        "tp_levels_requested": [103000.0],
        "trail_after_tp1_requested": False,
    }

    called_with: Dict[str, Any] = {}

    original_replace = ex._replace_position_protection

    def mock_replace(**kw):
        called_with.update(kw)
        return {"status": "ok", "side": "LONG", "qty": 0.001, "naked_ms": 5, "sl_algo_id": 100}

    ex._replace_position_protection = mock_replace

    result = ex.handle_modify({
        "sid": "sid-modify-1",
        "symbol": "BTCUSDT",
        # Note: no sl or tp_levels in payload
    })

    assert result.get("status") == "ok", f"result: {result}"
    # The sl and tps should have been resolved from state
    assert called_with.get("sl") == 98000.0, f"Expected sl=98000.0, got: {called_with.get('sl')}"
    assert called_with.get("tps") == [103000.0], f"Expected tps=[103000.0], got: {called_with.get('tps')}"


# =============================================================================
# Test 4: handle_resize returns status=flat when position closes to zero
# =============================================================================

def test_handle_resize_returns_flat_when_position_closed():
    """When read_live_position post-resize returns is_open=False (reduce-to-zero),
    handle_resize must return status=flat without calling _replace_position_protection.
    """
    ex = _mk_executor(position_amt=0.001, inspect_complete=True)

    # Seed state
    ex._state_cache["sid-resize-1"] = {
        "symbol": "BTCUSDT",
        "fsm_state": mod.FSM_PROTECTED,
        "side": "LONG",
        "qty": 0.001,
        "sl_requested": 98000.0,
        "tp_levels_requested": [103000.0],
    }

    # Mock _read_live_position: pre-resize open, post-resize flat
    call_count = {"n": 0}

    def mock_live_position(*, symbol, client):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return {"is_open": True, "qty": 0.001, "logical_side": "LONG", "position_amt": 0.001}
        # Second call — post resize: position is flat
        return {"is_open": False, "qty": 0.0, "logical_side": None, "position_amt": 0.0}

    ex._read_live_position = mock_live_position

    # Mock resize submission
    ex._submit_reduce_only_market_exit = lambda **kw: {"close_order_id": 999}

    replace_called = {"called": False}

    def mock_replace(**kw):
        replace_called["called"] = True
        return {"status": "ok"}

    ex._replace_position_protection = mock_replace

    result = ex.handle_resize({
        "sid": "sid-resize-1",
        "symbol": "BTCUSDT",
        "resize_mode": "delta_qty",
        "delta_qty": -0.001,  # full reduce
    })

    assert result.get("status") == "flat", f"Expected flat, got: {result}"
    assert not replace_called["called"], "_replace_position_protection must NOT be called when position is flat"
    state = ex._state_cache.get("sid-resize-1", {})
    assert state.get("fsm_state") == mod.FSM_EXIT_FILLED, f"FSM state: {state.get('fsm_state')}"
