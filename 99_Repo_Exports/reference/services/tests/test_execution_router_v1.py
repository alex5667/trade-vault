"""Unit tests for ExecutionRouter — intent→queue routing with scale-in redirect.

Tests:
1. Passthrough when router disabled
2. Passthrough for non-open actions
3. Open→resize redirect when conditions met
4. Same-side check blocks opposite-side
5. Budget check blocks over-budget adds
6. Max legs check blocks excessive legs
7. No existing position → passthrough
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Env setup before import
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ["EXEC_ROUTER_ENABLE"] = "1"
os.environ["EXEC_ROUTER_SCALE_IN_ENABLE"] = "1"
os.environ["EXEC_ROUTER_SAME_SIDE_ONLY"] = "1"
os.environ["EXEC_ROUTER_REQUIRE_OWNER_STABLE"] = "1"
os.environ["EXEC_ROUTER_REQUIRE_WCL_BUDGET"] = "0"  # skip WCL for most tests
os.environ["EXEC_ROUTER_RISK_BUDGET_USDT"] = "50"
os.environ["EXEC_ROUTER_MAX_LEGS"] = "3"

import pytest

from services.execution_router import ExecutionRouter


# --- FakeRedis ---

class FakeRedis:
    def __init__(self):
        self.store: Dict[str, str] = {}
        self.lists: Dict[str, List[str]] = {}
        self.stream: List[tuple] = []

    def get(self, key: str) -> Optional[bytes]:
        v = self.store.get(key)
        return v.encode() if isinstance(v, str) else v

    def set(self, key: str, value: str, ex: int = None) -> None:
        self.store[key] = value

    def rpush(self, key: str, *values: str) -> int:
        if key not in self.lists:
            self.lists[key] = []
        for v in values:
            self.lists[key].append(v)
        return len(self.lists[key])

    def blpop(self, key: str, timeout: int = 0) -> Optional[tuple]:
        lst = self.lists.get(key, [])
        if lst:
            return (key, lst.pop(0))
        return None

    def xadd(self, key: str, fields: dict, maxlen: int = None, approximate: bool = True) -> str:
        self.stream.append((key, dict(fields)))
        return "0-1"


def _mk_router(r: FakeRedis, **overrides) -> ExecutionRouter:
    router = ExecutionRouter(r)
    for k, v in overrides.items():
        setattr(router, k, v)
    return router


def _seed_guard(r: FakeRedis, symbol: str, sid: str, side: str = "LONG") -> None:
    """Seed an active symbol guard."""
    key = f"orders:active_symbol_sid:{symbol}"
    doc = {
        "sid": sid,
        "symbol": symbol,
        "side": side,
        "guard_status": "active",
        "guard_release_pending": False,
        "guard_version": 1,
    }
    r.store[key] = json.dumps(doc)


def _seed_state(r: FakeRedis, sid: str, **fields) -> None:
    """Seed order state."""
    key = f"orders:state:{sid}"
    state = {"sid": sid, **fields}
    r.store[key] = json.dumps(state)


# ===========================================================================
# Tests
# ===========================================================================

class TestRouterPassthrough:
    def test_passthrough_when_disabled(self):
        r = FakeRedis()
        router = _mk_router(r, enabled=True, scale_in_enabled=False)
        payload = json.dumps({"action": "open", "sid": "s1", "symbol": "BTCUSDT", "side": "LONG"})
        result = router.route_one(payload)
        assert result["status"] == "passthrough"
        assert r.lists.get("orders:queue:binance") == [payload]

    def test_passthrough_for_close_action(self):
        r = FakeRedis()
        router = _mk_router(r)
        payload = json.dumps({"action": "close", "sid": "s1", "symbol": "BTCUSDT"})
        result = router.route_one(payload)
        assert result["status"] == "passthrough"
        assert result["reason"] == "action=close"

    def test_passthrough_for_resize_action(self):
        r = FakeRedis()
        router = _mk_router(r)
        payload = json.dumps({"action": "resize", "sid": "s1", "symbol": "BTCUSDT"})
        result = router.route_one(payload)
        assert result["status"] == "passthrough"

    def test_passthrough_no_existing_position(self):
        r = FakeRedis()
        router = _mk_router(r)
        payload = json.dumps({"action": "open", "sid": "s1", "symbol": "BTCUSDT", "side": "LONG"})
        result = router.route_one(payload)
        assert result["status"] == "passthrough"
        assert result["reason"] == "no_existing_position"

    def test_bad_json_passthrough(self):
        r = FakeRedis()
        router = _mk_router(r)
        result = router.route_one("not-json{}")
        assert result["status"] == "passthrough"
        assert result["reason"] == "bad_json"


class TestScaleInRedirect:
    def test_open_to_resize_redirect(self):
        r = FakeRedis()
        router = _mk_router(r)
        _seed_guard(r, "BTCUSDT", "owner-1", side="LONG")
        _seed_state(r, "owner-1", symbol="BTCUSDT", side="LONG",
                    exec_price=100000, qty=0.001, fsm_state="PROTECTED",
                    sl_requested=98000, tp_levels_requested=[102000, 104000, 106000])

        payload = json.dumps({
            "action": "open", "sid": "new-signal-1", "symbol": "BTCUSDT",
            "side": "LONG", "qty": 0.001, "entry": 100500,
            "sl": 98000, "tp_levels": [102000, 104000, 106000],
        })
        result = router.route_one(payload)

        assert result["status"] == "scale_in"
        assert result["owner_sid"] == "owner-1"

        # Check that a resize payload was pushed to exec queue
        queue = r.lists.get("orders:queue:binance", [])
        assert len(queue) == 1
        resize = json.loads(queue[0])
        assert resize["action"] == "resize"
        assert resize["sid"] == "owner-1"
        assert resize["resize_mode"] == "delta_qty"
        assert resize["delta_qty"] == 0.001
        assert resize["scale_in_seq"] == 1
        assert resize["source_signal_id"] == "new-signal-1"
        assert resize["owner_sid"] == "owner-1"

    def test_opposite_side_blocks(self):
        r = FakeRedis()
        router = _mk_router(r)
        _seed_guard(r, "BTCUSDT", "owner-1", side="LONG")
        _seed_state(r, "owner-1", symbol="BTCUSDT", side="LONG",
                    exec_price=100000, qty=0.001)

        payload = json.dumps({
            "action": "open", "sid": "new-signal-2", "symbol": "BTCUSDT",
            "side": "SHORT", "qty": 0.001,
        })
        result = router.route_one(payload)

        assert result["status"] == "passthrough"
        assert result["reason"] == "opposite_side"

    def test_max_legs_blocks(self):
        r = FakeRedis()
        router = _mk_router(r, max_legs=2)
        _seed_guard(r, "BTCUSDT", "owner-1", side="LONG")
        _seed_state(r, "owner-1", symbol="BTCUSDT", side="LONG",
                    exec_price=100000, qty=0.001, scale_in_seq=1)

        payload = json.dumps({
            "action": "open", "sid": "s3", "symbol": "BTCUSDT",
            "side": "LONG", "qty": 0.001,
        })
        result = router.route_one(payload)

        assert result["status"] == "passthrough"
        assert result["reason"] == "max_legs_exceeded"

    def test_released_guard_passthrough(self):
        """Released tombstone → no existing position → passthrough."""
        r = FakeRedis()
        router = _mk_router(r)
        key = "orders:active_symbol_sid:BTCUSDT"
        r.store[key] = json.dumps({
            "sid": "old-1", "guard_status": "released",
        })

        payload = json.dumps({
            "action": "open", "sid": "s4", "symbol": "BTCUSDT",
            "side": "LONG", "qty": 0.001,
        })
        result = router.route_one(payload)
        assert result["status"] == "passthrough"
        assert result["reason"] == "no_existing_position"


class TestScaleInTpSchema:
    def test_resize_payload_includes_tp_qtys(self):
        """When TP levels are available, resize payload should include tp_qtys_requested_json."""
        r = FakeRedis()
        router = _mk_router(r)
        _seed_guard(r, "ETHUSDT", "owner-eth-1", side="LONG")
        _seed_state(r, "owner-eth-1", symbol="ETHUSDT", side="LONG",
                    exec_price=3000, qty=0.1, sl_requested=2900,
                    tp_levels_requested=[3100, 3200, 3300])

        payload = json.dumps({
            "action": "open", "sid": "new-eth-1", "symbol": "ETHUSDT",
            "side": "LONG", "qty": 0.05, "entry": 3050,
            "tp_levels": [3100, 3200, 3300],
        })
        result = router.route_one(payload)
        assert result["status"] == "scale_in"

        resize = json.loads(r.lists["orders:queue:binance"][0])
        assert "tp_qtys_requested_json" in resize
        tp_qtys = json.loads(resize["tp_qtys_requested_json"])
        assert len(tp_qtys) == 3
        # TP1 should close the new leg (0.05)
        assert tp_qtys[0] == pytest.approx(0.05)
        # Total should equal combined qty
        assert sum(tp_qtys) == pytest.approx(0.15)
        assert resize.get("trail_activate_tp_level_requested") == 1
