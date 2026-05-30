from __future__ import annotations

from types import SimpleNamespace

import pytest

from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate
from signals.level_enricher import attach_trade_levels_to_ctx


def test_strict_missing_levels_after_enricher_floor_skip(monkeypatch: pytest.MonkeyPatch):
    """
    Enricher skips attaching levels (stop_bps too small < floor) →
    strict gate must veto with REASON_MISSING_LEVELS.
    """
    monkeypatch.setenv("EDGE_COST_GATE_ENABLED", "1")
    monkeypatch.setenv("EDGE_COST_STRICT_MISSING_LEVELS", "1")
    monkeypatch.setenv("EDGE_COST_APPLY_KINDS", "breakout")
    monkeypatch.setenv("EDGE_EXPECTED_MOVE_MODE", "tp1")
    monkeypatch.setenv("EDGE_COST_K", "4.0")
    monkeypatch.setenv("EDGE_FEES_BPS_DEFAULT", "8.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_BPS_DEFAULT", "4.0")
    monkeypatch.setenv("EDGE_SLIPPAGE_USE_SPREAD_HALF", "0")
    # Deterministic TS/EMA env
    monkeypatch.setenv("EDGE_DISABLE_EMA", "1")
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")
    monkeypatch.setenv("SL_FLOOR_DEFAULT_BPS", "0")

    # floors: stop must be >= 10 bps → enricher will SKIP attaching levels
    monkeypatch.setenv("EDGE_LEVELS_MIN_STOP_BPS", "10")
    monkeypatch.setenv("EDGE_LEVELS_MIN_TP1_BPS", "0")

    gate = EdgeCostGate.from_env()

    ctx = SimpleNamespace()
    ctx.price = 100.0

    cfg = {
        "STOP_MODE": "ATR",
        "STOP_ATR_MULT": 0.05,  # stop_bps=5 < 10 -> skip attach
        "TP_MODE": "RR",
        "TP_RR": "1",
        "TP_ATR_MULTS": "0.6,1.0,1.5",
    }
    attach_trade_levels_to_ctx(ctx, side="LONG", symbol="BTCUSDT", cfg=cfg, overwrite=True, logger=None)

    d = gate.evaluate(ctx=ctx, kind="breakout", symbol="BTCUSDT")
    assert d.apply is True
    assert d.veto is True
    assert d.reason_code == EdgeCostGate.REASON_MISSING_LEVELS
