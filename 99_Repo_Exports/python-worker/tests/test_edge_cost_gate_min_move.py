from __future__ import annotations

from types import SimpleNamespace

import pytest

from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate


def test_edge_gate_min_expected_move_floor(monkeypatch: pytest.MonkeyPatch):
    """
    EDGE_MIN_EXPECTED_MOVE_BPS=30 => if expected_move < 30 => veto VETO_EDGE_TOO_SMALL.

    Setup: entry=100, tp1=100.20 => expected_move=20 bps < floor=30 bps => veto.
    fees=0, slippage=0, K=1 => thr=0 => passes below_k check.
    Min move floor catches it: veto VETO_EDGE_TOO_SMALL.
    """
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_COST_APPLY_KINDS", "")  # apply to all
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_K", "1.0")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "0.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "0.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "0")
    # Deterministic TS/EMA env
    monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")
    # Hard floor = 30 bps
    monkeypatch.setenv("EDGE_MIN_EXPECTED_MOVE_BPS", "30")

    gate = EdgeCostGate.from_env()

    ctx = SimpleNamespace()
    ctx.entry_price = 100.0
    ctx.tp1_price = 100.20  # 20 bps

    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    assert d.apply is True
    assert d.veto is True
    assert d.reason_code == "VETO_EDGE_TOO_SMALL"


def test_edge_gate_min_expected_move_floor_symbol_override(monkeypatch: pytest.MonkeyPatch):
    """
    Per-symbol min floor: EDGE_MIN_EXPECTED_MOVE_BPS_BTCUSDT=50 overrides default=0.
    """
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_K", "1.0")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "0.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "0.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "0")
    monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")
    monkeypatch.setenv("EDGE_MIN_EXPECTED_MOVE_BPS", "0")           # default: off
    monkeypatch.setenv("EDGE_MIN_EXPECTED_MOVE_BPS_BTCUSDT", "50")  # BTC: 50 bps floor

    gate = EdgeCostGate.from_env()

    ctx = SimpleNamespace(entry_price=100.0, tp1_price=100.30)  # 30 bps
    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    # 30 < 50 => veto
    assert d.veto is True
    assert d.reason_code == "VETO_EDGE_TOO_SMALL"

    # ETH doesn't have override → uses default=0 → no min floor → passes
    ctx2 = SimpleNamespace(entry_price=100.0, tp1_price=100.30)
    d2 = gate.evaluate(ctx=ctx2, kind="breakout", symbol="ETHUSDT")
    assert d2.veto is False


def test_edge_gate_min_expected_move_floor_pass_when_sufficient(monkeypatch: pytest.MonkeyPatch):
    """expected_move=50 >= floor=30 => pass (no VETO_EDGE_TOO_SMALL)."""
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_K", "1.0")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "0.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "0.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "0")
    monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")
    monkeypatch.setenv("EDGE_MIN_EXPECTED_MOVE_BPS", "30")

    gate = EdgeCostGate.from_env()
    ctx = SimpleNamespace(entry_price=100.0, tp1_price=100.50)  # 50 bps
    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    assert d.veto is False
    assert d.reason_code == EdgeCostGate.REASON_OK
