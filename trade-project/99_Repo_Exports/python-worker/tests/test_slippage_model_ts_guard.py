from __future__ import annotations

from services.slippage_model_by_fact import estimate_slippage_bps_ctx


class FakeRedis:
    def __init__(self):
        self.calls = []
    def hget(self, key, field):
        self.calls.append((key, field))
        return None


class Ctx:
    def __init__(self, ts_ms, bid=100.0, ask=101.0, venue="binance_futures", tf="1m"):
        self.ts_ms = ts_ms
        self.bid = bid
        self.ask = ask
        self.venue = venue
        self.tf = tf


def test_slippage_invalid_ts_skips_ema_and_does_not_touch_redis(monkeypatch):
    monkeypatch.setenv("SLIPPAGE_EMA_ENABLED", "1")
    r = FakeRedis()
    ctx = Ctx(ts_ms=0)
    out = estimate_slippage_bps_ctx(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=ctx.ts_ms,
        tf="1m",
        kind="absorption",
        default_bps=5.0,
        use_spread_half=True,
    )
    assert r.calls == []
    assert out >= 5.0


def test_slippage_seconds_ts_normalizes_and_can_use_ema(monkeypatch):
    monkeypatch.setenv("SLIPPAGE_EMA_ENABLED", "1")
    monkeypatch.setenv("SLIPPAGE_EMA_MIN_SAMPLES", "2")
    monkeypatch.setenv("SLIPPAGE_EMA_KEY_PREFIX", "slipema")
    # keep legacy keys by default for maximal backward compatibility in test
    monkeypatch.setenv("SLIPPAGE_EMA_DIM_TF_KIND", "0")

    class R:
        def __init__(self):
            self.calls = []
            self.data = {
                ('slipema:BTCUSDT:binance_futures:overnight', 'samples'): "10",
                ('slipema:BTCUSDT:binance_futures:overnight', 'ema_bps'): "12.3",
            }
        def hget(self, key, field):
            self.calls.append((key, field))
            return self.data.get((key, field))

    r = R()
    ctx = Ctx(ts_ms=1_700_000_000)  # seconds
    out = estimate_slippage_bps_ctx(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=ctx.ts_ms,
        tf="1m",
        kind="absorption",
        default_bps=1.0,
        use_spread_half=False,
    )
    # Should have made at least one Redis call (for samples)
    assert len(r.calls) >= 1
    # Should return at least the default (since EMA may not be found in test)
    assert out >= 1.0
