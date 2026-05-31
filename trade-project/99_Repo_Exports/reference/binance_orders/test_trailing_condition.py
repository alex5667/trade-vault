from __future__ import annotations

import os

from services.trailing_condition import TrailingConditionEvaluator
from services.ev_giveback_stats import GivebackEmaConfig


class FakeRedis:
    def __init__(self):
        self.h = {}

    def hgetall(self, key):
        return dict(self.h.get(key, {}))

    def hset(self, key, mapping=None, **kwargs):
        m = mapping or {}
        self.h.setdefault(key, {})
        self.h[key].update(m)


class Ctx:
    pass


def test_trailing_enabled_by_momentum(monkeypatch):
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")
    monkeypatch.setenv("TRAIL_COND_KINDS", "breakout")
    monkeypatch.setenv("TRAIL_COND_Z_THR", "2.0")
    monkeypatch.setenv("TRAIL_COND_OBI_THR", "0.10")
    monkeypatch.setenv("TRAIL_COND_USE_GIVEBACK_EMA", "0")

    r = FakeRedis()
    ev = TrailingConditionEvaluator(r)

    ctx = Ctx()
    ctx.z_delta = 2.5
    ctx.obi_avg = 0.0
    dec = ev.evaluate(ctx, side="LONG", symbol="BTCUSDT", kind="breakout", tf="1m", regime="na")
    assert dec.enabled is True
    assert dec.momentum_ok is True


def test_trailing_enabled_by_giveback_ema(monkeypatch):
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")
    monkeypatch.setenv("TRAIL_COND_KINDS", "breakout")
    monkeypatch.setenv("TRAIL_COND_USE_GIVEBACK_EMA", "1")
    monkeypatch.setenv("TRAIL_COND_GIVEBACK_BPS_MIN", "20")
    monkeypatch.setenv("TRAIL_COND_GIVEBACK_MIN_SAMPLES", "30")

    r = FakeRedis()
    gb_cfg = GivebackEmaConfig.from_env()
    key = gb_cfg.key(kind="breakout", symbol="BTCUSDT", tf="1m", regime="na")
    r.hset(key, mapping={"samples": "50", "ema_giveback_bps": "35.0"})

    ev = TrailingConditionEvaluator(r)

    ctx = Ctx()
    ctx.z_delta = 0.5
    ctx.obi_avg = 0.01
    dec = ev.evaluate(ctx, side="LONG", symbol="BTCUSDT", kind="breakout", tf="1m", regime="na")
    assert dec.enabled is True
    assert dec.giveback_risk_ok is True


def test_trailing_disabled_if_no_momentum_and_no_giveback(monkeypatch):
    monkeypatch.setenv("TRAIL_COND_ENABLED", "1")
    monkeypatch.setenv("TRAIL_COND_KINDS", "breakout")
    monkeypatch.setenv("TRAIL_COND_USE_GIVEBACK_EMA", "1")
    monkeypatch.setenv("TRAIL_COND_GIVEBACK_BPS_MIN", "999")
    monkeypatch.setenv("TRAIL_COND_GIVEBACK_MIN_SAMPLES", "30")

    r = FakeRedis()
    ev = TrailingConditionEvaluator(r)
    ctx = Ctx()
    ctx.z_delta = 0.1
    ctx.obi_avg = 0.01
    dec = ev.evaluate(ctx, side="LONG", symbol="BTCUSDT", kind="breakout", tf="1m", regime="na")
    assert dec.enabled is False
