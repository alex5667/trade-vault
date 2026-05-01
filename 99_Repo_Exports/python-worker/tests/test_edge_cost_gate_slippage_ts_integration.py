from __future__ import annotations
"""
test_edge_cost_gate_slippage_ts_integration.py

Integration tests for timestamp normalization + EMA slippage lookup in EdgeCostGate.

Key invariants:
  1. ts=0 (invalid) → EMA must be skipped, base slippage returned
  2. ts in seconds → normalize to ms → EMA can be used
  3. EMA key includes symbol×venue×session×tf×kind dimensions
"""

import pytest
from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate


def _pick_non_ev_mode():
    try:
        from handlers.crypto_orderflow.utils.edge_cost_gate import ExpectedMoveMode  # type: ignore
        for name in ("TP1", "tp1"):
            if hasattr(ExpectedMoveMode, name):
                return getattr(ExpectedMoveMode, name)
        for v in ExpectedMoveMode:  # type: ignore
            if str(getattr(v, "value", v)).lower() != "ev":
                return v
    except Exception:
        pass
    return "tp1"


class FakeRedisHgetall:
    """
    FakeRedis that implements hgetall() — used by gate's _load_slippage_ema_bps().
    Also implements hgetall that returns {} for drift:active:* keys (fail-open).
    """
    def __init__(self, *, ema_key: str, ema_bps: float = 20.0, samples: int = 100):
        self.ema_key = ema_key
        self.ema_bps = float(ema_bps)
        self.samples = int(samples)
        self.slipema_calls: list = []

    def hgetall(self, key: str) -> dict:
        if key == self.ema_key:
            self.slipema_calls.append(key)
            return {
                "samples": str(self.samples),
                "ema_slippage_bps": str(self.ema_bps),
            }
        # All other keys (drift:active:*, tca:*) → empty dict (fail-open)
        return {}

    def get(self, key: str):
        return None


class Ctx:
    def __init__(self, *, ts_ms, session="na", tf="1m", kind=None, signal_kind=None, strategy=None):
        self.ts_ms = ts_ms
        self.session = session
        self.tf = tf
        self.kind = kind
        self.signal_kind = signal_kind
        self.strategy = strategy
        self.symbol = "BTCUSDT"
        self.venue = "binance_futures"
        self.entry_price = 100.0
        self.tp1_price = 101.0
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
        slippage_bps_default=1.0,
        slippage_use_spread_half=False,
        min_expected_move_bps_default=0.0,
        min_expected_move_bps_by_symbol={},
        ev_p_min=0.0,
        ev_p_min_by_kind={},
        ev_min_trades=0,
        ev_strict_missing_stats=False,
        ev_dynamic_k_enabled=False,
        ev_dynamic_k_atr_mult=0.0,
    )


def test_invalid_ts_never_consults_slipema_redis(monkeypatch):
    """ts=0 → gate must NOT use EMA slippage (base only). drift overlay allowed to check redis."""
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_DISABLE_EMA", "0")
    monkeypatch.setenv("EDGE_SLIP_EMA_MIN_SAMPLES", "2")
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")  # prevent drift overlay redis calls

    r = FakeRedisHgetall(ema_key="slipema:BTCUSDT:binance_futures:na:1m:absorption")
    ctx = Ctx(ts_ms=0, tf="1m", strategy="absorption")
    ctx.redis = r

    g = _make_gate()
    d = g.evaluate(ctx=ctx, kind="absorption", symbol="BTCUSDT")

    # EMA slipema key must NOT have been called for invalid ts
    assert r.slipema_calls == [], f"slipema Redis should not be called with invalid ts, but got: {r.slipema_calls}"
    # Base slippage should be returned (1.0 default, no spread)
    assert d.slippage_bps >= 1.0


def test_seconds_ts_normalizes_and_uses_extended_tf_kind_key(monkeypatch):
    """
    ts in seconds → normalize to ms → session×tf×kind key → EMA used.

    We pre-set ctx.session to match the EMA key, avoiding session detection dependency.
    ts=1_735_725_600 sec → ~2025-01-01, skew vs now ~5.4M ms < 6h threshold → valid.
    """
    import time

    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "correct_skip_ema")
    monkeypatch.setenv("EDGE_DISABLE_EMA", "0")
    monkeypatch.setenv("EDGE_SLIP_EMA_MIN_SAMPLES", "2")
    monkeypatch.setenv("EDGE_TS_MAX_SKEW_MS", "21600000")  # 6h
    monkeypatch.setenv("EDGE_DRIFT_TIGHTEN", "0")  # No drift overlay

    sess = "na"
    tf = "1m"
    knd = "absorption"
    expected_key = f"slipema:BTCUSDT:binance_futures:{sess}:{tf}:{knd}"

    now_s = int(time.time())
    r = FakeRedisHgetall(ema_key=expected_key, ema_bps=20.0, samples=100)
    ctx = Ctx(ts_ms=now_s, session=sess, tf=tf, strategy=knd)  # seconds input
    ctx.redis = r

    g = _make_gate()
    d = g.evaluate(ctx=ctx, kind=knd, symbol="BTCUSDT")

    # EMA=20 > base=1.0 → slippage must be 20 (or at least > base)
    assert d.slippage_bps >= 10.0, f"Expected EMA slippage ~20, got {d.slippage_bps}"
    # Redis must have been called with the expected slipema key
    assert len(r.slipema_calls) > 0, f"Expected Redis call with slipema key, got no calls"
