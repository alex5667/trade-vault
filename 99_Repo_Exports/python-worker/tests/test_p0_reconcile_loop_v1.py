"""P0-2: Position reconcile loop — unit tests.

Tests cover:
1. No mismatches on clean state (exchange flat, local empty)
2. Naked position detected: exchange has position, no protection orders
3. Naked position with grace period: not flagged during grace
4. Orphan order: open order, no local position
5. Qty mismatch detection
6. Local-only position detection
7. Shadow mode: no auto-close called
8. Enforce + auto_close: flatten triggered on naked
9. Binance 429-style error (get_position_risk throws): loop survives
10. Symbols allowlist filters correctly
"""
from __future__ import annotations

import sys
import os
import time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# FakeRedis
# ---------------------------------------------------------------------------

class FakeRedis:
    def __init__(self, open_positions: dict | None = None):
        self._kv: dict = {}
        self._sets: dict = {}
        # Pre-populate orders:open if given
        if open_positions:
            self._sets["orders:open"] = set(open_positions.keys())
            for sid, pos in open_positions.items():
                self._kv[f"order:{sid}"] = pos

    def sscan(self, key, cursor, count=1000):
        members = list(self._sets.get(key, set()))
        return 0, [m.encode() if isinstance(m, str) else m for m in members]

    def hgetall(self, key):
        raw = self._kv.get(key) or {}
        return {
            k.encode() if isinstance(k, str) else k:
            v.encode() if isinstance(v, str) else v
            for k, v in raw.items()
        }

    def xadd(self, stream, fields, maxlen=None, approximate=True):
        return b"0-1"

    def set(self, key, val, ex=None, px=None):
        self._kv[key] = val

    def get(self, key):
        return self._kv.get(key)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_exchange_pos(symbol: str, amt: float) -> dict:
    return {"symbol": symbol, "positionAmt": str(amt), "markPrice": "50000.0",
            "unRealizedProfit": "0.0", "isolatedMargin": "100.0"}


def _make_sl_order(symbol: str) -> dict:
    return {"symbol": symbol, "type": "STOP_MARKET", "orderId": 1001,
            "origQty": "0.001", "status": "NEW"}


def _make_tp_order(symbol: str) -> dict:
    return {"symbol": symbol, "type": "TAKE_PROFIT_MARKET", "orderId": 1002,
            "origQty": "0.001", "status": "NEW"}


def _make_loop(
    exchange_positions=None,
    exchange_orders=None,
    local_positions=None,
    enforce=False,
    auto_close_naked=False,
    naked_grace_ms=3000,
    symbols_allowlist=None,
    flatten_svc=None,
):
    from services.position_reconcile_loop_v1 import PositionReconcileLoop

    # Build FakeRedis with local positions
    r = FakeRedis(open_positions=local_positions)

    # Mock client
    client = MagicMock()
    client.get_position_risk.return_value = list((exchange_positions or {}).values())
    client.get_open_orders.return_value = [
        o for orders in (exchange_orders or {}).values() for o in orders
    ]

    events = []

    def _write_event(fields):
        events.append(fields)

    loop = PositionReconcileLoop(
        r=r,
        binance_client=client,
        flatten_service=flatten_svc,
        filters=MagicMock(),
        enforce=enforce,
        auto_close_naked=auto_close_naked,
        naked_grace_ms=naked_grace_ms,
        symbols_allowlist=symbols_allowlist,
        write_event_fn=_write_event,
    )
    return loop, events


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestNoMismatch:
    def test_empty_state_no_mismatches(self):
        loop, events = _make_loop()
        mismatches = loop.run_once()
        assert mismatches == []

    def test_protected_position_no_mismatch(self):
        sym = "BTCUSDT"
        loop, events = _make_loop(
            exchange_positions={sym: _make_exchange_pos(sym, 0.001)},
            exchange_orders={sym: [_make_sl_order(sym), _make_tp_order(sym)]},
            local_positions={"sid-1": {"symbol": sym, "qty": "0.001", "status": "open"}},
        )
        mismatches = loop.run_once()
        naked = [m for m in mismatches if m["type"] == "naked_position"]
        assert naked == []


class TestNakedPositionDetection:
    def test_exchange_pos_no_local_is_naked(self):
        sym = "ETHUSDT"
        loop, events = _make_loop(
            exchange_positions={sym: _make_exchange_pos(sym, 0.01)},
            exchange_orders={},  # no protection
            local_positions={},   # no local state
        )
        mismatches = loop.run_once()
        naked = [m for m in mismatches if m["type"] == "naked_position"]
        assert len(naked) == 1
        assert naked[0]["symbol"] == sym

    def test_exchange_pos_with_local_no_protection_is_naked(self):
        sym = "SOLUSDT"
        loop, events = _make_loop(
            exchange_positions={sym: _make_exchange_pos(sym, 1.0)},
            exchange_orders={sym: []},  # no protection orders
            local_positions={"sid-2": {"symbol": sym, "qty": "1.0", "status": "open"}},
        )
        mismatches = loop.run_once()
        naked = [m for m in mismatches if m["type"] == "naked_position"]
        assert len(naked) == 1

    def test_grace_period_suppresses_naked(self):
        sym = "BTCUSDT"
        fill_ts = int(time.time() * 1000) - 500  # 500ms ago = within 3000ms grace
        loop, events = _make_loop(
            exchange_positions={sym: _make_exchange_pos(sym, 0.001)},
            exchange_orders={sym: []},
            local_positions={"sid-3": {
                "symbol": sym, "qty": "0.001", "status": "open",
                "fill_ts_ms": str(fill_ts),
            }},
            naked_grace_ms=3000,
        )
        mismatches = loop.run_once()
        naked = [m for m in mismatches if m["type"] == "naked_position"]
        assert naked == []

    def test_expired_grace_period_flags_naked(self):
        sym = "BTCUSDT"
        fill_ts = int(time.time() * 1000) - 10_000  # 10s ago > 3s grace
        loop, events = _make_loop(
            exchange_positions={sym: _make_exchange_pos(sym, 0.001)},
            exchange_orders={sym: []},
            local_positions={"sid-4": {
                "symbol": sym, "qty": "0.001", "status": "open",
                "fill_ts_ms": str(fill_ts),
            }},
            naked_grace_ms=3000,
        )
        mismatches = loop.run_once()
        naked = [m for m in mismatches if m["type"] == "naked_position"]
        assert len(naked) == 1


class TestOrphanOrderDetection:
    def test_orphan_order_no_position(self):
        sym = "PEPEUSDT"
        loop, events = _make_loop(
            exchange_positions={},
            exchange_orders={sym: [_make_sl_order(sym)]},
            local_positions={},
        )
        mismatches = loop.run_once()
        orphans = [m for m in mismatches if m["type"] == "orphan_order"]
        assert len(orphans) == 1
        assert orphans[0]["symbol"] == sym


class TestQtyMismatch:
    def test_qty_mismatch_above_threshold(self):
        sym = "BTCUSDT"
        loop, events = _make_loop(
            exchange_positions={sym: _make_exchange_pos(sym, 0.010)},
            exchange_orders={sym: [_make_sl_order(sym)]},
            local_positions={"sid-5": {"symbol": sym, "qty": "0.020", "status": "open"}},
        )
        mismatches = loop.run_once()
        qty_mm = [m for m in mismatches if m["type"] == "qty_mismatch"]
        assert len(qty_mm) == 1
        assert qty_mm[0]["diff_pct"] > 0.05

    def test_qty_mismatch_within_threshold_no_flag(self):
        sym = "ETHUSDT"
        loop, events = _make_loop(
            exchange_positions={sym: _make_exchange_pos(sym, 0.100)},
            exchange_orders={sym: [_make_sl_order(sym)]},
            local_positions={"sid-6": {"symbol": sym, "qty": "0.101", "status": "open"}},
        )
        mismatches = loop.run_once()
        qty_mm = [m for m in mismatches if m["type"] == "qty_mismatch"]
        assert qty_mm == []


class TestLocalOnlyPosition:
    def test_local_only_detected(self):
        sym = "SOLUSDT"
        loop, events = _make_loop(
            exchange_positions={},   # exchange says flat
            exchange_orders={},
            local_positions={"sid-7": {"symbol": sym, "qty": "5.0", "status": "open"}},
        )
        mismatches = loop.run_once()
        lo = [m for m in mismatches if m["type"] == "local_only_position"]
        assert len(lo) == 1
        assert lo[0]["symbol"] == sym


class TestEnforceAutoClose:
    def test_shadow_no_flatten_called(self):
        sym = "BTCUSDT"
        flatten_svc = MagicMock()
        loop, events = _make_loop(
            exchange_positions={sym: _make_exchange_pos(sym, 0.001)},
            exchange_orders={},
            local_positions={},
            enforce=False,
            auto_close_naked=False,
            flatten_svc=flatten_svc,
        )
        loop.run_once()
        flatten_svc.force_flatten_exact.assert_not_called()

    def test_enforce_auto_close_calls_flatten(self):
        sym = "ETHUSDT"
        flatten_svc = MagicMock()
        flatten_svc.force_flatten_exact.return_value = {"flatten_ok": True}
        # Need client.get_position_risk(symbol=...) to return position
        from services.position_reconcile_loop_v1 import PositionReconcileLoop
        r = FakeRedis()
        client = MagicMock()
        client.get_position_risk.return_value = [_make_exchange_pos(sym, 0.01)]
        client.get_open_orders.return_value = []
        events = []
        loop = PositionReconcileLoop(
            r=r,
            binance_client=client,
            flatten_service=flatten_svc,
            filters=MagicMock(),
            enforce=True,
            auto_close_naked=True,
            naked_grace_ms=0,
            write_event_fn=lambda f: events.append(f),
        )
        loop.run_once()
        flatten_svc.force_flatten_exact.assert_called_once()
        call_kwargs = flatten_svc.force_flatten_exact.call_args.kwargs
        assert call_kwargs["symbol"] == sym
        assert call_kwargs["reason"] == "reconcile_naked_position"


class TestResilience:
    def test_exchange_error_does_not_crash(self):
        from services.position_reconcile_loop_v1 import PositionReconcileLoop
        r = FakeRedis()
        client = MagicMock()
        client.get_position_risk.side_effect = Exception("429 Too Many Requests")
        client.get_open_orders.side_effect = Exception("network error")
        loop = PositionReconcileLoop(r=r, binance_client=client)
        mismatches = loop.run_once()
        # Should return empty, not raise
        assert isinstance(mismatches, list)

    def test_symbols_allowlist_filters(self):
        sym_in = "BTCUSDT"
        sym_out = "PEPEUSDT"
        loop, events = _make_loop(
            exchange_positions={
                sym_in: _make_exchange_pos(sym_in, 0.001),
                sym_out: _make_exchange_pos(sym_out, 1000.0),
            },
            exchange_orders={},
            local_positions={},
            symbols_allowlist={sym_in},
        )
        mismatches = loop.run_once()
        symbols_flagged = {m["symbol"] for m in mismatches}
        assert sym_in in symbols_flagged
        assert sym_out not in symbols_flagged
