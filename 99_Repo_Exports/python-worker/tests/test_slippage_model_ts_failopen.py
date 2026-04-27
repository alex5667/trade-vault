from __future__ import annotations

from services.slippage_model import SlippageModelConfig, estimate_slippage_bps


class Tick:
    def __init__(self, bid: float, ask: float):
        self.bid = bid
        self.ask = ask


class Ctx:
    def __init__(self, ts_ms=None, ts=None, venue="binance_futures", tf="1m"):
        self.ts_ms = ts_ms
        self.ts = ts
        self.venue = venue
        self.tf = tf


class FakeRedis:
    def __init__(self):
        self.calls = []

    def hget(self, key, field):
        self.calls.append((key, field))
        return None


def test_slippage_invalid_ts_skips_ema_and_does_not_touch_redis():
    cfg = SlippageModelConfig(
        enabled=True,
        default_slippage_bps=2.0,
        half_spread_mult=0.5,
        use_ema=True,
        ema_min_samples=10,
        key_prefix="slipema",
    )
    r = FakeRedis()
    ctx = Ctx(ts_ms=0)
    tick = Tick(100.0, 101.0)

    out = estimate_slippage_bps(
        cfg=cfg,
        ctx=ctx,
        tick=tick,
        symbol="BTCUSDT",
        venue="binance_futures",
        tf="1m",
        kind="absorption",
        redis_client=r,
    )
    assert r.calls == []
    assert out > 0


def test_slippage_seconds_ts_normalizes_to_ms_and_ema_can_be_used():
    cfg = SlippageModelConfig(
        enabled=True,
        default_slippage_bps=1.0,
        half_spread_mult=0.0,
        use_ema=True,
        ema_min_samples=2,
        key_prefix="slipema",
    )

    class R:
        def __init__(self):
            self.calls = []
        def hget(self, key, field):
            self.calls.append((key, field))
            if field == "samples":
                return "10"
            if field == "ema_bps":
                return "12.3"
            return None

    r = R()
    ctx = Ctx(ts_ms=1_700_000_000)  # seconds
    tick = Tick(100.0, 100.01)

    out = estimate_slippage_bps(
        cfg=cfg,
        ctx=ctx,
        tick=tick,
        symbol="BTCUSDT",
        venue="binance_futures",
        tf="1m",
        kind="absorption",
        redis_client=r,
    )
    assert len(r.calls) >= 2
    assert out >= 12.3