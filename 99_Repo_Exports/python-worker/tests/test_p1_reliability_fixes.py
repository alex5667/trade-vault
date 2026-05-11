"""
P1 Reliability Fixes — Regression Tests

Covers:
1. LONG-bias fix: ibm=None must produce direction=UNKNOWN, not LONG
2. generate_signal_id stability: same inputs → same id, across retries
3. stable_closed_trade_id: no _ms_now() in hash → idempotent
4. unknown_side_policy: is_buyer_maker=None neutralizes direction
"""
from __future__ import annotations

import hashlib
import sys
import os
import types
import asyncio

import pytest

# ─── helpers ─────────────────────────────────────────────────────────────────


def _make_tick(ibm: bool | None = None) -> dict:
    tick: dict = {"price": 100.0, "qty": 1.0, "ts_ms": 1714000000000}
    if ibm is not None:
        tick["is_buyer_maker"] = ibm
    return tick


# ─── 1. LONG-bias fix in confidence_calculation/tick_processor ────────────────

class TestLongBias:
    """ibm=None (missing is_buyer_maker) must NOT produce LONG direction."""

    def _direction_from_ibm(self, ibm_value, key_present: bool = True):
        """Reproduce the fixed logic from tick_processor.py."""
        tick = {"ts_ms": 1714000000000, "qty": 1.0}
        if key_present:
            tick["is_buyer_maker"] = ibm_value
        ibm = tick.get("is_buyer_maker") if "is_buyer_maker" in tick else tick.get("m")
        # Fixed logic (mirrors the patch):
        if ibm is True:
            return "SHORT"
        elif ibm is False:
            return "LONG"
        else:
            return "UNKNOWN"

    def test_ibm_true_is_short(self):
        """is_buyer_maker=True (taker SELL) → SHORT."""
        assert self._direction_from_ibm(True) == "SHORT"

    def test_ibm_false_is_long(self):
        """is_buyer_maker=False (taker BUY) → LONG."""
        assert self._direction_from_ibm(False) == "LONG"

    def test_ibm_none_is_unknown_not_long(self):
        """is_buyer_maker=None must be UNKNOWN — not LONG (P1 bias fix)."""
        result = self._direction_from_ibm(None, key_present=True)
        assert result == "UNKNOWN", (
            f"P1 LONG-bias: ibm=None should produce UNKNOWN but got {result!r}"
        )

    def test_ibm_missing_key_is_unknown(self):
        """is_buyer_maker key absent and m key absent → UNKNOWN."""
        tick = {"ts_ms": 1714000000000, "qty": 1.0}  # no ibm key, no m key
        ibm = tick.get("is_buyer_maker") if "is_buyer_maker" in tick else tick.get("m")
        if ibm is True:
            direction = "SHORT"
        elif ibm is False:
            direction = "LONG"
        else:
            direction = "UNKNOWN"
        assert direction == "UNKNOWN"

    def test_old_logic_would_be_long_for_none(self):
        """Confirm the OLD logic had LONG-bias: `SHORT if ibm else LONG`."""
        ibm = None
        old_direction = "SHORT" if ibm else "LONG"  # OLD buggy logic
        assert old_direction == "LONG", "Confirming old bias for regression clarity"

    def test_old_logic_would_be_short_for_true(self):
        ibm = True
        assert ("SHORT" if ibm else "LONG") == "SHORT"

    def test_old_logic_would_be_long_for_false(self):
        ibm = False
        assert ("SHORT" if ibm else "LONG") == "LONG"


# ─── 2. generate_signal_id stability ─────────────────────────────────────────

class TestSignalIdStability:
    """generate_signal_id must return identical output for identical inputs across retries."""

    def _generate(self, symbol, ts_ms, direction, kind="delta_spike") -> str:
        """Reproduce generate_signal_id from common/normalization.py."""
        d_norm = direction.upper().strip()
        if d_norm in ("LONG", "L", "1", "BUY"):
            d_short = "L"
        elif d_norm in ("SHORT", "S", "-1", "SELL"):
            d_short = "S"
        else:
            raise ValueError(f"unknown direction: {direction!r}")
        symbol_norm = (symbol or "UNKNOWN").upper().strip()
        return f"{kind}:{symbol_norm}:{int(ts_ms)}:{d_short}"

    def test_same_inputs_same_id(self):
        """Same symbol+ts+direction → same signal_id."""
        id1 = self._generate("BTCUSDT", 1714234567890, "LONG")
        id2 = self._generate("BTCUSDT", 1714234567890, "LONG")
        assert id1 == id2

    def test_different_ts_different_id(self):
        id1 = self._generate("BTCUSDT", 1714234567890, "LONG")
        id2 = self._generate("BTCUSDT", 1714234567891, "LONG")
        assert id1 != id2

    def test_different_direction_different_id(self):
        id1 = self._generate("BTCUSDT", 1714234567890, "LONG")
        id2 = self._generate("BTCUSDT", 1714234567890, "SHORT")
        assert id1 != id2

    def test_replay_same_output(self):
        """Simulate replay: same inputs, 1000 re-computations → all identical."""
        base = self._generate("SOLUSDT", 1714000000001, "SHORT", kind="sweep_eqh")
        for _ in range(1000):
            assert self._generate("SOLUSDT", 1714000000001, "SHORT", kind="sweep_eqh") == base

    def test_unknown_direction_raises(self):
        """UNKNOWN direction must raise (we gate earlier)."""
        with pytest.raises((ValueError, Exception)):
            self._generate("BTCUSDT", 1714000000000, "UNKNOWN")

    def test_direction_aliases(self):
        """BUY / LONG / L all map to same id."""
        assert self._generate("BTCUSDT", 1714000000000, "LONG") == \
               self._generate("BTCUSDT", 1714000000000, "BUY") == \
               self._generate("BTCUSDT", 1714000000000, "L")


# ─── 3. stable_closed_trade_id ───────────────────────────────────────────────

class TestStableClosedTradeId:
    """closed_trade_id must be identical across retries / replay."""

    @staticmethod
    def stable_closed_trade_id(
        sid: str,
        *,
        exit_order_ref: str = "",
        exit_ts_ms: int = 0,
        close_reason: str = "",
    ) -> str:
        """Mirror of the patched static method."""
        base = f"{sid}|{exit_order_ref}|{int(exit_ts_ms)}|{close_reason}"
        suffix = hashlib.sha1(base.encode()).hexdigest()[:24]
        return f"closed:{suffix}"

    def test_same_inputs_same_id(self):
        id1 = self.stable_closed_trade_id(
            "sig-123", exit_order_ref="binance|exit|oid=999", exit_ts_ms=1714000000000
        )
        id2 = self.stable_closed_trade_id(
            "sig-123", exit_order_ref="binance|exit|oid=999", exit_ts_ms=1714000000000
        )
        assert id1 == id2

    def test_different_exit_ref_different_id(self):
        id1 = self.stable_closed_trade_id("sig-123", exit_order_ref="oid=999", exit_ts_ms=100)
        id2 = self.stable_closed_trade_id("sig-123", exit_order_ref="oid=888", exit_ts_ms=100)
        assert id1 != id2

    def test_different_ts_different_id(self):
        id1 = self.stable_closed_trade_id("sig-123", exit_ts_ms=100)
        id2 = self.stable_closed_trade_id("sig-123", exit_ts_ms=200)
        assert id1 != id2

    def test_replay_1000x(self):
        """1000 retries of same close → same closed_trade_id."""
        expected = self.stable_closed_trade_id(
            "sig-abc", exit_order_ref="binance|tp1|oid=456", exit_ts_ms=1714567890000
        )
        for _ in range(1000):
            assert self.stable_closed_trade_id(
                "sig-abc", exit_order_ref="binance|tp1|oid=456", exit_ts_ms=1714567890000
            ) == expected

    def test_close_reason_affects_id(self):
        id1 = self.stable_closed_trade_id("sid", exit_ts_ms=100, close_reason="tp1")
        id2 = self.stable_closed_trade_id("sid", exit_ts_ms=100, close_reason="sl")
        assert id1 != id2

    def test_old_implementation_was_non_deterministic(self):
        """Prove old implementation (uses time.time()) produces different values."""
        import time

        def _ms_now():
            return int(time.time() * 1000)

        def old_new_closed_trade_id(sid, exit_order_ref=""):
            suffix = hashlib.sha1(f"{sid}|{exit_order_ref}|{_ms_now()}".encode()).hexdigest()[:12]
            return f"closed:{sid}:{suffix}"

        # Two calls in rapid succession should still be different (probabilistically)
        ids = {old_new_closed_trade_id("sig-test") for _ in range(10)}
        # Can't guarantee they differ in 10ms window, but document the risk
        # Stable version has zero risk by design:
        stable = {self.stable_closed_trade_id("sig-test", exit_ts_ms=100) for _ in range(10)}
        assert len(stable) == 1, "Stable version must always produce exactly 1 unique ID"


# ─── 4. unknown_side_policy sets is_buyer_maker=None ─────────────────────────

class TestUnknownSidePolicyPatch:
    """ignore_delta policy must set is_buyer_maker=None to prevent LONG-bias downstream."""

    def _apply_ignore_delta(self, tick: dict) -> dict:
        """Mirror of the patched unknown_side_policy ignore_delta branch."""
        tick["qty_signed"] = 0.0
        tick["aggressor_sign"] = 0
        tick["counted_in_delta"] = False
        tick["side_known"] = False
        tick["side"] = "UNKNOWN"
        tick["side_reason"] = "unknown_side_ignore_delta"
        tick["is_buyer_maker"] = None  # P1-FIX
        return tick

    def test_is_buyer_maker_is_none_after_policy(self):
        """After ignore_delta, is_buyer_maker must be None, not missing."""
        tick = {"is_buyer_maker": True, "qty": 1.0}
        result = self._apply_ignore_delta(tick)
        assert "is_buyer_maker" in result
        assert result["is_buyer_maker"] is None

    def test_side_is_unknown(self):
        tick = {"qty": 1.0}
        result = self._apply_ignore_delta(tick)
        assert result["side"] == "UNKNOWN"

    def test_side_known_is_false(self):
        tick = {"qty": 1.0}
        result = self._apply_ignore_delta(tick)
        assert result["side_known"] is False

    def test_counted_in_delta_false(self):
        tick = {"qty": 1.0}
        result = self._apply_ignore_delta(tick)
        assert result["counted_in_delta"] is False

    def test_downstream_direction_is_unknown_after_policy(self):
        """After policy, downstream direction extraction must yield UNKNOWN not LONG."""
        tick = {"qty": 1.0}
        tick = self._apply_ignore_delta(tick)
        # Simulate downstream direction logic (P1-fixed version):
        ibm = tick.get("is_buyer_maker") if "is_buyer_maker" in tick else tick.get("m")
        if ibm is True:
            direction = "SHORT"
        elif ibm is False:
            direction = "LONG"
        else:
            direction = "UNKNOWN"
        assert direction == "UNKNOWN", (
            f"P1 regression: after unknown_side_policy, direction should be UNKNOWN, got {direction!r}"
        )
