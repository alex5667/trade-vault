from __future__ import annotations

"""
Unit tests for EdgeCostGate: RR mode and ATR mode.

tp1 mode is covered in test_edge_cost_gate.py and test_edge_cost_gate_integration.py.
EV mode is covered in test_edge_cost_gate_ev_mode.py.
This file fills the gap for rr and atr modes.
"""

from types import SimpleNamespace

import pytest

from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate


def _base_env(monkeypatch) -> None:
    """Deterministic, no-Redis ENV."""
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_COST_APPLY_KINDS", "")  # apply to all
    monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "0")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "8.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "4.0")
    monkeypatch.setenv("EDGE_COST_K", "4.0")
    monkeypatch.setenv("EDGE_COST_STRICT_MISSING_LEVELS", "0")

    # Disable new buffer logic so K*(fees+slip) exact math holds
    monkeypatch.setenv("EDGE_BUFFER_BASE_BPS", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_ATR_MULT", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_SPREAD_MULT", "0.0")


# ---------------------------------------------------------------------------
# RR mode
# ---------------------------------------------------------------------------

class TestRRMode:
    """
    rr mode: expected_move = |entry - sl| * rr
    threshold = K * (fees + slip) = 4 * (8 + 4) = 48 bps
    """

    def test_rr_mode_pass(self, monkeypatch):
        """
        entry=100, sl=99 → risk_bps=100; rr=1.0 → expected_move=100 bps.
        threshold=48 bps → pass (100 >= 48).
        """
        _base_env(monkeypatch)
        monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "rr")

        gate = EdgeCostGate.from_env()
        ctx = SimpleNamespace(entry_price=100.0, sl_price=99.0, tp_rr=1.0)
        dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
        assert dec.apply is True
        assert dec.veto is False
        assert dec.reason_code == EdgeCostGate.REASON_OK
        assert dec.expected_move_bps == pytest.approx(100.0, rel=1e-4)

    def test_rr_mode_veto(self, monkeypatch):
        """
        entry=100, sl=99.9 → risk=10 bps; rr=1.0 → expected_move=10 bps.
        threshold=48 bps → veto (10 < 48).
        """
        _base_env(monkeypatch)
        monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "rr")

        gate = EdgeCostGate.from_env()
        ctx = SimpleNamespace(entry_price=100.0, sl_price=99.9, tp_rr=1.0)
        dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
        assert dec.apply is True
        assert dec.veto is True
        assert dec.reason_code == EdgeCostGate.REASON_BELOW_K

    def test_rr_mode_with_high_rr_passes(self, monkeypatch):
        """
        entry=100, sl=99.9 (10 bps risk) but rr=6 → expected=60 bps → pass.
        """
        _base_env(monkeypatch)
        monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "rr")

        gate = EdgeCostGate.from_env()
        ctx = SimpleNamespace(entry_price=100.0, sl_price=99.9, tp_rr=6.0)
        dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
        assert dec.veto is False
        assert dec.expected_move_bps == pytest.approx(60.0, rel=1e-4)

    def test_rr_mode_with_rr_list_uses_first(self, monkeypatch):
        """rr_list=[2.0, 3.0] → uses first element (2.0) for expected_move."""
        _base_env(monkeypatch)
        monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "rr")

        gate = EdgeCostGate.from_env()
        ctx = SimpleNamespace(entry_price=100.0, sl_price=99.5, rr_list=[2.0, 3.0])
        # risk_bps = |100-99.5|/100*10000 = 50; expected = 50*2 = 100 bps
        dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
        assert dec.veto is False
        assert dec.expected_move_bps == pytest.approx(100.0, rel=1e-4)

    def test_rr_mode_missing_sl_fail_open(self, monkeypatch):
        """Missing sl → expected_move=NaN → fail-open (strict=0)."""
        _base_env(monkeypatch)
        monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "rr")

        gate = EdgeCostGate.from_env()
        ctx = SimpleNamespace(entry_price=100.0, tp_rr=1.5)
        dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
        assert dec.veto is False
        assert dec.reason_code == EdgeCostGate.REASON_OK
        assert "fail_open" in dec.notes

    def test_rr_mode_missing_sl_strict_veto(self, monkeypatch):
        """Missing sl → expected_move=NaN → fail-closed (strict=1)."""
        _base_env(monkeypatch)
        monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "rr")
        monkeypatch.setenv("EDGE_COST_STRICT_MISSING_LEVELS", "1")

        gate = EdgeCostGate.from_env()
        ctx = SimpleNamespace(entry_price=100.0, tp_rr=1.5)
        dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
        assert dec.veto is True
        assert dec.reason_code == EdgeCostGate.REASON_MISSING_LEVELS


# ---------------------------------------------------------------------------
# ATR mode
# ---------------------------------------------------------------------------

class TestATRMode:
    """
    atr mode: expected_move = atr * mult (absolute) → bps vs entry.
    threshold = K * (fees + slip) = 4 * (8 + 4) = 48 bps.
    """

    def test_atr_mode_pass(self, monkeypatch):
        """
        entry=100, atr=1.0, tp1_atr_mult=0.6 → move=0.6 → 60 bps → pass.
        """
        _base_env(monkeypatch)
        monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "atr")

        gate = EdgeCostGate.from_env()
        ctx = SimpleNamespace(entry_price=100.0, entry=100.0, atr=1.0, tp1_atr_mult=0.6)
        dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
        assert dec.apply is True
        assert dec.veto is False
        assert dec.reason_code == EdgeCostGate.REASON_OK
        assert dec.expected_move_bps == pytest.approx(60.0, rel=1e-4)

    def test_atr_mode_veto(self, monkeypatch):
        """
        entry=100, atr=0.1, mult=1.0 → move=0.1 → 10 bps (< 48) → veto.
        """
        _base_env(monkeypatch)
        monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "atr")

        gate = EdgeCostGate.from_env()
        ctx = SimpleNamespace(entry_price=100.0, entry=100.0, atr=0.1, tp1_atr_mult=1.0)
        dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
        assert dec.apply is True
        assert dec.veto is True
        assert dec.reason_code == EdgeCostGate.REASON_BELOW_K

    def test_atr_mode_uses_atr14_fallback(self, monkeypatch):
        """When ctx.atr absent, ctx.atr14 is used."""
        _base_env(monkeypatch)
        monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "atr")

        gate = EdgeCostGate.from_env()
        ctx = SimpleNamespace(entry_price=100.0, entry=100.0, atr14=1.0, tp1_atr_mult=0.6)
        dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
        assert dec.veto is False
        assert dec.expected_move_bps == pytest.approx(60.0, rel=1e-4)

    def test_atr_mode_uses_of_atr_fallback(self, monkeypatch):
        """When ctx.atr absent, ctx.of.atr is used."""
        _base_env(monkeypatch)
        monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "atr")

        gate = EdgeCostGate.from_env()
        of = SimpleNamespace(atr=1.0)
        ctx = SimpleNamespace(entry_price=100.0, entry=100.0, of=of, tp1_atr_mult=0.6)
        dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
        assert dec.veto is False
        assert dec.expected_move_bps == pytest.approx(60.0, rel=1e-4)

    def test_atr_mode_mult_from_tp_atr_mults_list(self, monkeypatch):
        """tp_atr_mults=[1.5, 2.0] → uses first element (1.5)."""
        _base_env(monkeypatch)
        monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "atr")

        gate = EdgeCostGate.from_env()
        ctx = SimpleNamespace(entry_price=100.0, entry=100.0, atr=1.0, tp_atr_mults=[1.5, 2.0])
        # move = 1.0 * 1.5 = 1.5 → 150 bps → pass
        dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
        assert dec.veto is False
        assert dec.expected_move_bps == pytest.approx(150.0, rel=1e-4)

    def test_atr_mode_missing_atr_fail_open(self, monkeypatch):
        """Missing atr → NaN → fail-open (strict=0)."""
        _base_env(monkeypatch)
        monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "atr")

        gate = EdgeCostGate.from_env()
        ctx = SimpleNamespace(entry_price=100.0)
        dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
        assert dec.veto is False
        assert dec.reason_code == EdgeCostGate.REASON_OK


# ---------------------------------------------------------------------------
# ctx.of spread fallback (cross-mode)
# ---------------------------------------------------------------------------

class TestCtxOfSpreadFallback:
    """Verify spread_bps is correctly extracted from ctx.of when absent on ctx."""

    def test_spread_from_ctx_of_spread_bps(self, monkeypatch):
        """ctx has no spread_bps, ctx.of.spread_bps=20 → slippage=max(4,10)=10."""
        monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
        monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
        monkeypatch.setenv("EDGE_COST_K", "4.0")
        monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "8.0")
        monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "4.0")
        monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "1")
        monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
        monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")
        monkeypatch.setenv("EDGE_BUFFER_BASE_BPS", "0.0")
        monkeypatch.setenv("EDGE_BUFFER_ATR_MULT", "0.0")
        monkeypatch.setenv("EDGE_BUFFER_SPREAD_MULT", "0.0")

        gate = EdgeCostGate.from_env()
        of = SimpleNamespace(spread_bps=20.0)
        ctx = SimpleNamespace(entry_price=100.0, tp1_price=100.50, of=of)

        dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
        # slippage = max(4, 20/2) = 10
        # threshold = 4 * (8 + 10) = 72
        assert dec.slippage_bps == pytest.approx(10.0)
        assert dec.threshold_bps == pytest.approx(72.0)

    def test_spread_from_ctx_of_ask_bid(self, monkeypatch):
        """ctx.of has ask/bid (no spread_bps) → computed spread used."""
        monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
        monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
        monkeypatch.setenv("EDGE_COST_K", "4.0")
        monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "8.0")
        monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "4.0")
        monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "1")
        monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
        monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")

        gate = EdgeCostGate.from_env()
        # ask=100.2, bid=99.8 → spread=(0.4/100)*10000=40 bps → half=20
        of = SimpleNamespace(ask=100.2, bid=99.8)
        ctx = SimpleNamespace(entry_price=100.0, tp1_price=101.0, of=of)

        dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
        # slippage = max(4, 20) = 20
        assert dec.slippage_bps == pytest.approx(20.0, abs=0.5)

    def test_ctx_spread_takes_priority_over_ctx_of(self, monkeypatch):
        """ctx.spread_bps=10 takes priority over ctx.of.spread_bps=40."""
        monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
        monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
        monkeypatch.setenv("EDGE_COST_K", "4.0")
        monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "8.0")
        monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "4.0")
        monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "1")
        monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
        monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")

        gate = EdgeCostGate.from_env()
        of = SimpleNamespace(spread_bps=40.0)
        # ctx has spread_bps=10 → half=5; should NOT use of.spread_bps
        ctx = SimpleNamespace(entry_price=100.0, tp1_price=101.0, spread_bps=10.0, of=of)

        dec = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
        # slippage = max(4, 10/2) = 5 (from ctx, not ctx.of)
        assert dec.slippage_bps == pytest.approx(5.0)
