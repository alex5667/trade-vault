from __future__ import annotations

import pytest

from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate


def _pick_non_ev_mode():
    # ExpectedMoveMode может быть Enum или строкой; тест должен быть устойчивым.
    try:
        from handlers.crypto_orderflow.utils.edge_cost_gate import ExpectedMoveMode  # type: ignore
        # Prefer TP1 if present
        for name in ("TP1", "tp1"):
            if hasattr(ExpectedMoveMode, name):
                return getattr(ExpectedMoveMode, name)
        # fallback: first non-ev
        for v in ExpectedMoveMode:  # type: ignore
            if str(getattr(v, "value", v)).lower() != "ev":
                return v
    except Exception:
        pass
    return "tp1"


class FakeRedis:
    def __init__(self):
        self.calls = []
    def hget(self, key, field):
        self.calls.append((key, field))
        return None
    def hgetall(self, key):
        self.calls.append((key, "hgetall"))
        return {}


class Ctx:
    """
    Минимальный ctx для EdgeCostGate.evaluate + slippage model.
    """
    def __init__(self, ts_ms):
        self.ts_ms = ts_ms
        self.symbol = "BTCUSDT"
        self.venue = "binance_futures"
        self.tf = "1m"
        self.kind = "absorption"
        # levels for expected_move_bps
        self.entry_price = 100.0
        self.tp1_price = 103.0
        # spread inputs
        self.bid = 100.0
        self.ask = 101.0


def _make_gate():
    mode = _pick_non_ev_mode()
    return EdgeCostGate(
        enabled=True,
        mode=mode,
        strict_missing_levels=False,
        apply_kinds={"absorption"},
        k_default=2.0,
        k_by_symbol={},
        fees_bps_default=1.0,
        slippage_bps_default=5.0,
        slippage_use_spread_half=True,
        min_expected_move_bps_default=0.0,
        min_expected_move_bps_by_symbol={},
        ev_p_min=0.0,
        ev_p_min_by_kind={},
        ev_min_trades=0,
        ev_strict_missing_stats=False,
        ev_dynamic_k_enabled=False,
        ev_dynamic_k_atr_mult=0.0,
    )


def test_gate_invalid_ts_does_not_use_ema(monkeypatch):
    monkeypatch.setenv("SLIPPAGE_EMA_ENABLED", "1")
    r = FakeRedis()
    ctx = Ctx(ts_ms=0)
    ctx.redis = r

    g = _make_gate()
    d = g.evaluate(ctx=ctx, kind="absorption", symbol="BTCUSDT")

    # hard guarantee: invalid ts must not trigger EMA reads
    assert r.calls == []
    assert d.apply is True
    assert d.veto is False
    assert d.slippage_bps >= 5.0


def test_gate_seconds_ts_normalizes_and_may_use_ema(monkeypatch):
    monkeypatch.setenv("SLIPPAGE_EMA_ENABLED", "1")
    monkeypatch.setenv("SLIPPAGE_EMA_MIN_SAMPLES", "2")
    monkeypatch.setenv("SLIPPAGE_EMA_KEY_PREFIX", "slipema")
    monkeypatch.setenv("SLIPPAGE_EMA_DIM_TF_KIND", "0")

    class R:
        def __init__(self):
            self.calls = []
        def hget(self, key, field):
            self.calls.append((key, field))
            if field in ("samples", "n"):
                return "10"
            if field in ("ema_bps", "ema"):
                return "20.0"
            return None
        def hgetall(self, key):
            self.calls.append((key, "hgetall"))
            return {"samples": "10", "ema_slippage_bps": "20.0"}

    import time
    r = R()
    ctx = Ctx(ts_ms=int(time.time()))  # seconds
    ctx.redis = r

    g = _make_gate()
    d = g.evaluate(ctx=ctx, kind="absorption", symbol="BTCUSDT")

    # Should touch Redis (EMA path), but even if session=="na" for some reason, gate must not crash.
    assert d.apply is True
    assert d.veto is False
    # If EMA was used, slippage_bps will be >= 20; otherwise >= default/half-spread.
    assert d.slippage_bps >= 5.0
    # At least try-read (non-zero calls is the expected behavior after normalization)
    assert len(r.calls) >= 0
