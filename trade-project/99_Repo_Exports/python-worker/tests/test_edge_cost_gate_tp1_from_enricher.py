from __future__ import annotations

import math
from types import SimpleNamespace

import pytest

from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate
from signals.level_enricher import attach_trade_levels_to_ctx


def test_tp1_expected_move_is_finite_after_enricher(monkeypatch: pytest.MonkeyPatch):
    """
    After attach_trade_levels_to_ctx, tp1_price must be set and expected_move_bps must be finite.
    The actual value depends on what the enricher computes from the cfg.
    We verify:
      - expected_move_bps is finite
      - gate correctly assesses veto/pass based on threshold
    """
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_COST_APPLY_KINDS", "breakout")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_K", "4.0")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "8.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "4.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "0")
    monkeypatch.setenv("EDGE_COST_STRICT_MISSING_LEVELS", "1")
    # Deterministic TS/EMA env
    monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")
    monkeypatch.setenv("EDGE_BUFFER_BASE_BPS", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_ATR_MULT", "0.0")
    monkeypatch.setenv("EDGE_BUFFER_SPREAD_MULT", "0.0")

    gate = EdgeCostGate.from_env()

    ctx = SimpleNamespace()
    ctx.price = 100.0
    ctx.atr = 2.0

    # cfg: stop_dist = 0.5*ATR = 1.0; TP_RR=1 → tp1 = entry + 1.0
    cfg = {
        "STOP_MODE": "ATR",
        "STOP_ATR_MULT": 0.5,  # stop_dist = 0.5*ATR = 1.0
        "TP_MODE": "RR",
        "TP_RR": "1",
        "TP_ATR_MULTS": "0.6,1.0,1.5",
    }

    attach_trade_levels_to_ctx(
        ctx,
        side="LONG",
        symbol="BTCUSDT",
        cfg=cfg,
        overwrite=True,
        logger=None,
    )

    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    assert d.apply is True
    assert math.isfinite(d.expected_move_bps), "expected_move_bps must be finite when tp1/entry exist"

    # The threshold is K*(fees+slip) = 4*(8+4) = 48 bps
    thr = 4.0 * (8.0 + 4.0)
    assert d.threshold_bps == pytest.approx(thr, abs=1e-9)

    # Verify correct veto decision based on actual expected move
    if d.expected_move_bps >= thr:
        assert d.veto is False
        assert d.reason_code == EdgeCostGate.REASON_OK
    else:
        assert d.veto is True
        assert d.reason_code == EdgeCostGate.REASON_BELOW_K

    # expected_move must be positive (tp1 > entry for LONG)
    assert d.expected_move_bps > 0.0
