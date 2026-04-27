from utils.time_utils import get_ny_time_millis
import os
import time
from types import SimpleNamespace

from domain.time_utils import session_from_ts_ms
from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps

class FakeRedis:
    def __init__(self):
        self.h = {}
        self.calls = 0

    def hgetall(self, key):
        self.calls += 1
        return self.h.get(key, {})

def test_ema_v2_used_when_valid_ts(monkeypatch):
    monkeypatch.setenv("EDGE_TS_BAD_POLICY", "allow")
    monkeypatch.setenv("EDGE_SLIP_EMA_MIN_SAMPLES", "2")

    r = FakeRedis()
    now = get_ny_time_millis()
    sess = (session_from_ts_ms(now) or "na").lower()

    key = f"slipema:BTCUSDT:binance:{sess}:1m:absorption"
    r.h[key] = {"samples": "10", "ema_slip_bps": "80.0"}

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
    assert v >= 80.0  # EMA dominates base here
