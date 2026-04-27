from __future__ import annotations

from types import SimpleNamespace
import math
import pytest

from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate


@pytest.fixture(autouse=True)
def _base_env(monkeypatch):
    """Deterministic ENV: disable EMA, drift tightening."""
    monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")
    monkeypatch.setenv("EDGE_BUFFER_BASE_BPS", "0.0")


def test_tp1_mode_uses_tp1_price(monkeypatch):
    """tp1 mode: expected=20 bps < threshold=K*(fees+slip)=4*(8+4)=48 => veto."""
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_K", "4.0")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "8.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "4.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "0")

    gate = EdgeCostGate.from_env()
    ctx = SimpleNamespace(entry_price=100.0, tp1_price=100.3)
    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert d.apply is True
    assert d.expected_move_bps == pytest.approx(30.0, rel=1e-9)
    assert d.threshold_bps == pytest.approx(48.0, rel=1e-9)
    assert d.veto is True
    assert d.reason_code == EdgeCostGate.REASON_BELOW_K


def test_rr_mode_computes_from_risk_times_rr(monkeypatch):
    """rr mode: sl=99 => risk=100 bps; rr=2 => expected=200 bps; thr=0 => pass."""
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "rr")
    monkeypatch.setenv("EDGE_COST_K", "1.0")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "0.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "0.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "0")

    gate = EdgeCostGate.from_env()
    ctx = SimpleNamespace(entry_price=100.0, sl_price=99.0, tp_rr=2.0)
    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert d.veto is False
    assert d.expected_move_bps == pytest.approx(200.0, rel=1e-9)


def test_spread_half_slippage(monkeypatch):
    """spread_bps=20 on ctx => slippage = max(4, 10)=10."""
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_K", "1.0")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "0.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "4.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "1")

    gate = EdgeCostGate.from_env()
    # ctx.spread_bps=20 => half=10 > default 4 => slippage=10
    ctx = SimpleNamespace(entry_price=100.0, tp1_price=100.2, spread_bps=20.0)
    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert d.slippage_bps == pytest.approx(10.0, rel=1e-9)


def test_strict_missing_levels_vetoes(monkeypatch):
    """strict=1 + no tp1/sl/atr => veto REASON_MISSING_LEVELS."""
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_STRICT_MISSING_LEVELS", "1")

    gate = EdgeCostGate.from_env()
    ctx = SimpleNamespace(entry_price=100.0)
    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert d.apply is True
    assert d.veto is True
    assert d.reason_code == EdgeCostGate.REASON_MISSING_LEVELS


def test_strict_missing_levels_false_fails_open(monkeypatch):
    """strict=0 + no tp1/sl/atr => fail-open (veto=False)."""
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_STRICT_MISSING_LEVELS", "0")

    gate = EdgeCostGate.from_env()
    ctx = SimpleNamespace(entry_price=100.0)
    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert d.apply is True
    assert d.veto is False
    assert d.reason_code == EdgeCostGate.REASON_OK


def test_rr_list_scalar(monkeypatch):
    """rr mode reads first element from rr_list[] or scalar tp_rr."""
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "rr")
    monkeypatch.setenv("EDGE_COST_K", "1.0")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "0.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "0.0")

    gate = EdgeCostGate.from_env()

    # rr_list: take first
    ctx1 = SimpleNamespace(entry_price=100.0, sl_price=99.0, rr_list=[2.0, 3.0])
    d1 = gate.evaluate(ctx=ctx1, kind="breakout", symbol="BTCUSDT")
    assert d1.expected_move_bps == pytest.approx(200.0, rel=1e-9)

    # tp_rr scalar
    ctx2 = SimpleNamespace(entry_price=100.0, sl_price=99.0, tp_rr=3.0)
    d2 = gate.evaluate(ctx=ctx2, kind="breakout", symbol="BTCUSDT")
    assert d2.expected_move_bps == pytest.approx(300.0, rel=1e-9)


def test_tp_levels_as_list(monkeypatch):
    """tp1 mode: extracts tp1_price from tp_levels[0]."""
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_K", "1.0")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "0.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "0.0")

    gate = EdgeCostGate.from_env()
    ctx = SimpleNamespace(entry_price=100.0, tp_levels=[100.5, 101.0, 101.5])
    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")

    assert d.expected_move_bps == pytest.approx(50.0, rel=1e-9)  # |100.5-100|/100*10000
    assert d.veto is False
