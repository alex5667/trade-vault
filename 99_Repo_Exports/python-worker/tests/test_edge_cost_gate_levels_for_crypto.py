from __future__ import annotations

import math
from types import SimpleNamespace

import pytest

from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate
from signals.level_enricher import attach_trade_levels_to_ctx


def _base_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_COST_APPLY_KINDS", "breakout,extreme,absorption")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_K", "4.0")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "8.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "4.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "0")
    # Deterministic TS/EMA: prevent ts=None hitting veto slippage
    monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")
    monkeypatch.setenv("EDGE_BUFFER_BASE_BPS", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_ATR_MULT", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_SPREAD_MULT", "0.0")


def test_strict_missing_levels_veto_then_ok_after_enricher(monkeypatch: pytest.MonkeyPatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("EDGE_COST_STRICT_MISSING_LEVELS", "1")

    gate = EdgeCostGate.from_env()

    # ctx without trade levels -> strict must veto as REASON_MISSING_LEVELS
    ctx = SimpleNamespace()
    ctx.price = 100.0

    d0 = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    assert d0.apply is True
    assert d0.veto is True
    assert d0.reason_code == EdgeCostGate.REASON_MISSING_LEVELS
    assert not math.isfinite(d0.expected_move_bps)

    # Attach levels: ATR=2, STOP_ATR_MULT=0.5 -> stop_dist=1.0, TP_RR=1 -> tp1=entry+1=101
    ctx.atr = 2.0
    cfg = {
        "STOP_MODE": "ATR",
        "STOP_ATR_MULT": 0.5,
        "STOP_PCT": 0.2,
        "STOP_POINTS": 1.0,
        "TP_MODE": "RR",
        "TP_RR": "1",
        "TP_ATR_MULTS": "0.6,1.0,1.5",
    }
    attach_trade_levels_to_ctx(ctx, side="LONG", symbol="BTCUSDT", cfg=cfg, overwrite=True, logger=None)

    d1 = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    assert d1.apply is True
    assert math.isfinite(d1.expected_move_bps)
    # thr = 4*(8+4)=48 => if expected_move > 48 => pass
    if d1.expected_move_bps >= 48.0:
        assert d1.veto is False
        assert d1.reason_code == EdgeCostGate.REASON_OK
    else:
        # Still a valid veto-by-cost, not missing levels
        assert d1.reason_code == EdgeCostGate.REASON_BELOW_K


def test_apply_kinds_respected(monkeypatch: pytest.MonkeyPatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("EDGE_COST_APPLY_KINDS", "breakout")  # only breakout

    gate = EdgeCostGate.from_env()
    ctx = SimpleNamespace()
    ctx.entry_price = 100.0
    ctx.tp1_price = 101.0

    d = gate.evaluate(ctx=ctx, kind="obi_spike", symbol="BTCUSDT")
    assert d.apply is False
    assert d.veto is False


def test_spread_half_slippage(monkeypatch: pytest.MonkeyPatch):
    """
    Verify spread_bps on ctx drives slippage = max(default, spread_bps/2).
    spread=20 → slippage=max(4,10)=10; threshold=4*(8+10)=72 bps.
    tp1=100.50 → expected=50 bps < 72 → veto REASON_BELOW_K.
    Uses direct SimpleNamespace to avoid enricher tp1 uncertainty.
    """
    _base_env(monkeypatch)
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "1")

    gate = EdgeCostGate.from_env()

    # Manual ctx: entry=100, spread=20 → slippage=10; thr=72; expected=50 → veto
    ctx = SimpleNamespace(
        entry_price=100.0,
        tp1_price=100.50,
        spread_bps=20.0,
    )
    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    assert d.apply is True
    assert d.slippage_bps == pytest.approx(10.0, abs=0.5)
    assert d.threshold_bps == pytest.approx(72.0, abs=0.5)
    assert d.expected_move_bps == pytest.approx(50.0, abs=0.5)
    assert d.veto is True
    assert d.reason_code == EdgeCostGate.REASON_BELOW_K


def test_spread_half_no_spread_uses_default(monkeypatch: pytest.MonkeyPatch):
    """When no spread info in ctx, slippage falls back to default=4."""
    _base_env(monkeypatch)
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "1")

    gate = EdgeCostGate.from_env()
    ctx = SimpleNamespace(entry_price=100.0, tp1_price=100.50)  # no spread_bps
    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    # slippage = max(4, 0) = 4
    assert d.slippage_bps == pytest.approx(4.0, abs=0.5)
    assert d.threshold_bps == pytest.approx(48.0, abs=0.5)
