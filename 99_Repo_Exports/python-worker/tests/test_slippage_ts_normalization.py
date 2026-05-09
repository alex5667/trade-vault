import math

import pytest


class _RedisMock:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def hgetall(self, key):
        self.calls.append(key)
        # emulate redis returning bytes
        out = {}
        for k, v in self.payload.items():
            out[k.encode()] = str(v).encode()
        return out


class _Ctx:
    def __init__(self, *, bid, ask, ts_ms=None, ts=None, tf="1m", kind="na", session=None, venue="binance_futures"):
        self.bid = bid
        self.ask = ask
        self.ts_ms = ts_ms
        self.ts = ts
        self.tf = tf
        self.kind = kind
        self.session = session
        self.venue = venue


def test_estimate_slippage_invalid_ts_skips_ema(monkeypatch):
    """
    If ts_ms <= 0 => session='na' and EMA must NOT be used (no Redis call).
    """
    from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps

    class _RedisFail:
        def hgetall(self, key):
            raise AssertionError("Redis must not be called when ts is invalid")

    ctx = _Ctx(bid=100.0, ask=101.0, ts_ms=0)
    v = estimate_slippage_bps(
        ctx,
        redis_client=_RedisFail(),
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=0,
        default_bps=5.0,
        use_spread_half=True,
    )
    # spread_bps ~= 99.5 => half ~ 49.75, must dominate default 5
    assert v > 40.0


def test_estimate_slippage_seconds_ts_normalizes(monkeypatch):
    """
    If ts_ms is accidentally in epoch seconds (<1e12) it must be normalized to ms.
    We don't assert session name (timezone dependent); we assert:
      - function does not crash
      - Redis is called (EMA path enabled)
      - returned value can reflect EMA when it dominates spread/default
    """
    from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps

    # Very small spread so EMA wins.
    ctx = _Ctx(bid=100.00, ask=100.01, ts=1_700_000_000, ts_ms=None, tf="1m", kind="breakout")

    r = _RedisMock({"samples": 50, "ema_bps": 12.0})
    v = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=ctx.ts,  # seconds on purpose
        default_bps=5.0,
        use_spread_half=True,
    )
    assert len(r.calls) >= 1
    assert math.isfinite(v)
    # with spread ~ 1bps => half ~ 0.5, default 5, EMA 12 => expect 12
    assert v == pytest.approx(12.0, rel=1e-6)
