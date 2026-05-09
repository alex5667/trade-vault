from __future__ import annotations

"""Tests: SL fallback and placement failure alerting.

Tests:
1. SL dropped by validator → fallback SL computed and placed
2. SL valid → no fallback triggered
3. SL dropped + fallback_pct=0 → no fallback (disabled)
4. SL placement exception → Telegram + critical exec_event
5. TP placement exception → Telegram + critical exec_event
"""

import importlib.util
import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.redis_keys import RedisStreams as RS


# --- Env setup before module import ---
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ["EXEC_SL_FALLBACK_PCT"] = "1.0"

mod_path = Path(__file__).parent.parent / "binance_executor.py"
spec = importlib.util.spec_from_file_location("binance_executor_sl_fb", mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


# --- Fakes ---

class FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}
        self.stream: list = []

    def get(self, key: str) -> bytes | None:
        v = self.store.get(key)
        return v.encode() if v else None

    def set(self, key: str, value: str, ex: int = None) -> None:
        self.store[key] = value

    def xadd(self, key: str, fields: dict, maxlen: int = None, approximate: bool = True) -> str:
        self.stream.append((key, dict(fields)))
        return "0-1"

    def sismember(self, key: str, member: str) -> bool:
        return False


class FakeFilters:
    class _Inner:
        tick_size = 0.01
        step_size = 0.001
        min_qty = 0.001
        min_notional = 5.0
        max_qty = 10000.0
        price_precision = 2
        qty_precision = 3
    def get(self, symbol):
        return self._Inner()


class FakeClient:
    """Mock BinanceFuturesClient with configurable mark price and algo order results."""

    def __init__(self, *, mark_price: float = 2000.0, algo_result: dict = None,
                 algo_raise: Exception = None):
        self._mark_price = mark_price
        self._algo_result = algo_result or {"algoId": 123}
        self._algo_raise = algo_raise
        self.algo_calls: list = []

    def get_mark_price(self, symbol: str) -> float:
        return self._mark_price

    def post_algo_order(self, params: dict) -> dict:
        self.algo_calls.append(params)
        if self._algo_raise:
            raise self._algo_raise
        return dict(self._algo_result)

    def get_open_algo_orders(self, symbol=None) -> list:
        return []

    def get_position_risk(self) -> list:
        return []


def _mk_executor(**overrides) -> mod.BinanceExecutor:
    """Build a BinanceExecutor stub with minimal wiring."""
    ex = mod.BinanceExecutor.__new__(mod.BinanceExecutor)
    ex.r = FakeRedis()
    ex.redis = ex.r
    ex.exec_stream = RS.ORDERS_EXEC
    ex.orders_state_prefix = "orders:state:"
    ex.orders_state_ttl = 86400
    ex.state_key_prefix = "orders:state:"
    ex.state_ttl = 86400
    ex.allowlist = {"BTCUSDT", "ETHUSDT"}
    ex.position_mode = "oneway"
    ex.protection_arm_timeout_ms = 2500
    ex.sl_fallback_pct = 1.0
    ex.sl_working_type = "MARK_PRICE"
    ex.tp_market_working_type = "MARK_PRICE"
    ex.tp_limit_trigger_working_type = "MARK_PRICE"
    ex.trail_working_type = "MARK_PRICE"
    ex.fill_timeout_s = 10.0
    ex.tg = None  # will override in tests that check Telegram
    ex.demo_client = None
    ex.client = None

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

    # Stubs
    ex._exec_events = []
    def _exec_ev(ev):
        ex._exec_events.append(ev)
        ex.r.xadd(ex.exec_stream, {k: str(v) for k, v in ev.items()})
    ex._exec_event = _exec_ev
    ex._transition_state = lambda sid, *, symbol, action, next_state, details=None: _save(sid, {"fsm_state": next_state, **(details or {})})

    # Reconcile stubs
    ex.reconcile_enable = False
    ex._reconcile_user_stream_status = lambda *a, **kw: None

    for k, v in overrides.items():
        setattr(ex, k, v)

    return ex


# ============================================================
# Test 1: SL dropped → fallback applied
# ============================================================

def test_sl_fallback_applied_when_signal_sl_crossed():
    """When signal SL is wildly crossed (dropped by validator),
    _place_protective should compute fallback SL from mark ± 1% and place it."""
    mark = 2000.0
    client = FakeClient(mark_price=mark)
    filters = FakeFilters()
    ex = _mk_executor()

    # Policy stub
    policy = MagicMock()
    policy.name = "SAFETY_FIRST"
    policy.reason = "default"
    policy.tp_watchdog_enabled = False
    policy.tp_working_type = "MARK_PRICE"

    # Signal SL for SHORT that is wildly below mark (wrong side)
    # For SHORT, SL must be > mark. SL=1900 is far below mark=2000 → dropped
    result = ex._place_protective(
        sid="test-sid-1", symbol="ETHUSDT", logical_side="SHORT",
        qty=0.5, sl=1900.0, tps=[],
        policy=policy, client=client, filters=filters,
        ref_price=2000.0,
    )

    # Fallback SL should be mark * 1.01 = 2020.0
    assert result.get("sl_algo_id") == 123, f"SL should have been placed: {result}"
    # Check that sl_fallback event was emitted
    fallback_events = [e for e in ex._exec_events if e.get("action") == "sl_fallback"]
    assert len(fallback_events) == 1
    assert fallback_events[0]["sl_original"] == 1900.0
    assert fallback_events[0]["mark_price"] == 2000.0


# ============================================================
# Test 2: SL valid → no fallback
# ============================================================

def test_no_fallback_when_sl_is_valid():
    """When signal SL is valid (correct side of mark), no fallback should trigger."""
    mark = 2000.0
    client = FakeClient(mark_price=mark)
    filters = FakeFilters()
    ex = _mk_executor()

    policy = MagicMock()
    policy.name = "SAFETY_FIRST"
    policy.reason = "default"
    policy.tp_watchdog_enabled = False
    policy.tp_working_type = "MARK_PRICE"

    # For SHORT, SL must be > mark. SL=2100 is valid.
    result = ex._place_protective(
        sid="test-sid-2", symbol="ETHUSDT", logical_side="SHORT",
        qty=0.5, sl=2100.0, tps=[],
        policy=policy, client=client, filters=filters,
        ref_price=2000.0,
    )

    assert result.get("sl_algo_id") == 123
    fallback_events = [e for e in ex._exec_events if e.get("action") == "sl_fallback"]
    assert len(fallback_events) == 0, "No fallback should be triggered for valid SL"


# ============================================================
# Test 3: SL dropped + fallback_pct=0 → no fallback
# ============================================================

def test_no_fallback_when_disabled():
    """When sl_fallback_pct=0, no fallback should be computed even if SL is dropped."""
    mark = 2000.0
    client = FakeClient(mark_price=mark)
    filters = FakeFilters()
    ex = _mk_executor(sl_fallback_pct=0.0)

    policy = MagicMock()
    policy.name = "SAFETY_FIRST"
    policy.reason = "default"
    policy.tp_watchdog_enabled = False
    policy.tp_working_type = "MARK_PRICE"

    # SL=1900 for SHORT is invalid → dropped, but fallback is disabled
    result = ex._place_protective(
        sid="test-sid-3", symbol="ETHUSDT", logical_side="SHORT",
        qty=0.5, sl=1900.0, tps=[],
        policy=policy, client=client, filters=filters,
        ref_price=2000.0,
    )

    # sl_algo_id should NOT be set (no SL placed, no fallback)
    assert result.get("sl_algo_id") in (None, "", 0), \
        "SL should not have been placed when fallback is disabled"
    fallback_events = [e for e in ex._exec_events if e.get("action") == "sl_fallback"]
    assert len(fallback_events) == 0


# ============================================================
# Test 4: SL placement exception → critical event + Telegram
# ============================================================

def test_sl_placement_failure_sends_telegram():
    """When SL algo order fails with exception, Telegram alert is sent."""
    client = FakeClient(mark_price=2000.0, algo_raise=RuntimeError("Binance -2021"))
    filters = FakeFilters()
    tg = MagicMock()
    ex = _mk_executor(tg=tg)

    policy = MagicMock()
    policy.name = "SAFETY_FIRST"
    policy.reason = "default"
    policy.tp_watchdog_enabled = False
    policy.tp_working_type = "MARK_PRICE"

    result = ex._place_protective(
        sid="test-sid-4", symbol="ETHUSDT", logical_side="SHORT",
        qty=0.5, sl=2100.0, tps=[],
        policy=policy, client=client, filters=filters,
        ref_price=2000.0,
    )

    # SL placement failed → sl_algo_id missing
    assert result.get("sl_algo_id") in (None, "", 0)

    # Exec event should have severity=critical + incident_tag
    sl_fail_events = [e for e in ex._exec_events if e.get("action") == "place_sl_failed"]
    assert len(sl_fail_events) == 1
    assert sl_fail_events[0]["severity"] == "critical"
    assert sl_fail_events[0]["incident_tag"] == "capital_protection"

    # Telegram should have been called
    tg.send_text.assert_called_once()
    msg = tg.send_text.call_args[0][0]
    assert "SL placement FAILED" in msg
    assert "ETHUSDT" in msg


# ============================================================
# Test 5: TP placement exception → critical event + Telegram
# ============================================================

def test_tp_placement_failure_sends_telegram():
    """When TP algo order fails with exception, Telegram alert is sent."""
    # SL succeeds, TP fails
    call_count = {"n": 0}
    class SelectiveClient(FakeClient):
        def post_algo_order(self, params):
            call_count["n"] += 1
            if call_count["n"] == 1:
                # SL succeeds
                return {"algoId": 100}
            # TP fails
            raise RuntimeError("Binance -4131 margin")

    client = SelectiveClient(mark_price=2000.0)
    filters = FakeFilters()
    tg = MagicMock()
    ex = _mk_executor(tg=tg)

    policy = MagicMock()
    policy.name = "SAFETY_FIRST"
    policy.reason = "default"
    policy.tp_watchdog_enabled = False
    policy.tp_working_type = "MARK_PRICE"

    result = ex._place_protective(
        sid="test-sid-5", symbol="ETHUSDT", logical_side="LONG",
        qty=0.5, sl=1900.0, tps=[2100.0],
        policy=policy, client=client, filters=filters,
        ref_price=2000.0,
    )

    # SL should succeed
    assert result.get("sl_algo_id") == 100

    # TP should fail
    assert result.get("tp1_algo_id") in (None, "", 0)

    # TP failure exec event
    tp_fail_events = [e for e in ex._exec_events if "place_tp" in (e.get("action", "")) and e.get("status") == "error"]
    assert len(tp_fail_events) == 1
    assert tp_fail_events[0]["severity"] == "critical"

    # Telegram called for TP failure (SL succeeded → one TG call for TP only)
    tg_calls = [c for c in tg.send_text.call_args_list if "TP" in str(c)]
    assert len(tg_calls) >= 1
