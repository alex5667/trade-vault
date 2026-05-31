from types import SimpleNamespace

from domain.time_utils import session_from_ts_ms
from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps
from utils.time_utils import get_ny_time_millis


class FakeRedis:
    def __init__(self):
        self.h = {}
        self.calls = 0

    def hgetall(self, key):
        self.calls += 1
        return self.h.get(key, {})

    def hset(self, key, mapping=None, **kw):
        m = dict(mapping or {})
        m.update(kw)
        self.h[key] = {**self.h.get(key, {}), **m}

def test_roundtrip_ema_used_when_valid_ts(monkeypatch):
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "allow")
    monkeypatch.setenv("EDGE_SLIP_EMA_MIN_SAMPLES", "2")

    r = FakeRedis()
    now = get_ny_time_millis()
    sess = session_from_ts_ms(now) or "na"

    # write "EMA state" directly (we only need to test reader+selector)
    key = f"slipema:BTCUSDT:binance:{sess}:1m:absorption"
    r.hset(key, mapping={"samples": "10", "ema_slip_bps": "80.0", "last_ts_ms": str(now)})

    ctx = SimpleNamespace(ts_ms=now, bid=100.0, ask=100.1, tf="1m", venue="binance")
    v = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance",
        ts_ms=now,
        kind="absorption",
        default_bps=5.0,
        use_spread_half=True,
    )
    assert r.calls > 0
    # half-spread here small (~5 bps), EMA is 80 => should choose EMA
    assert v >= 80.0
