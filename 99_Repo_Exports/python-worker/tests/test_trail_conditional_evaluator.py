from __future__ import annotations

from types import SimpleNamespace
from handlers.crypto_orderflow.utils.trail_conditional import TrailConditionalEvaluator


class FakeRedis:
    def __init__(self, d=None):
        self.d = d or {}
    def hget(self, key, field):
        return self.d.get((key, field))


def test_momentum_allows_trailing(monkeypatch):
    monkeypatch.setenv("TRAIL_COND_EVAL_ENABLED", "1")
    monkeypatch.setenv("TRAIL_USE_GIVEBACK_STATS", "0")
    ctx = SimpleNamespace(z_delta=2.0, obi_avg=0.5, obi_sustained=True)
    ev = TrailConditionalEvaluator.from_env(redis=None)
    dec = ev.evaluate(ctx, side="LONG", symbol="BTCUSDT", kind="breakout", tf="1m", regime="na")
    assert dec.enabled is True


def test_giveback_ema_allows_when_high(monkeypatch):
    monkeypatch.setenv("TRAIL_COND_EVAL_ENABLED", "1")
    monkeypatch.setenv("TRAIL_USE_GIVEBACK_STATS", "1")
    monkeypatch.setenv("TRAIL_VETO_IF_NO_STATS", "1")
    monkeypatch.setenv("TRAIL_GIVEBACK_R_MIN", "0.3")
    r = FakeRedis({("trailstats:breakout:BTCUSDT:1m:na", "ema_giveback_r"): "0.55"})
    ctx = SimpleNamespace(z_delta=0.1, obi_avg=0.0)
    ev = TrailConditionalEvaluator.from_env(redis=r)
    dec = ev.evaluate(ctx, side="LONG", symbol="BTCUSDT", kind="breakout", tf="1m", regime="na")
    assert dec.enabled is True


def test_no_stats_veto(monkeypatch):
    monkeypatch.setenv("TRAIL_COND_EVAL_ENABLED", "1")
    monkeypatch.setenv("TRAIL_USE_GIVEBACK_STATS", "1")
    monkeypatch.setenv("TRAIL_VETO_IF_NO_STATS", "1")
    ctx = SimpleNamespace(z_delta=0.1, obi_avg=0.0)
    ev = TrailConditionalEvaluator.from_env(redis=None)
    dec = ev.evaluate(ctx, side="LONG", symbol="BTCUSDT", kind="breakout", tf="1m", regime="na")
    assert dec.enabled is False
