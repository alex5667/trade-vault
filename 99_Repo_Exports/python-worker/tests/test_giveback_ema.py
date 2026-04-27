from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import time

from services.ev_giveback_stats import GivebackEmaConfig, update_giveback_ema, read_giveback_ema


class FakeRedis:
    def __init__(self):
        self.h = {}
        self.ttl = {}

    def hgetall(self, key):
        return dict(self.h.get(key, {}))

    def hset(self, key, mapping=None, **kwargs):
        m = mapping or {}
        self.h.setdefault(key, {})
        for k, v in m.items():
            self.h[key][k] = v

    def expire(self, key, ttl):
        self.ttl[key] = int(ttl)

    def pipeline(self, transaction=False):
        return self

    def execute(self):
        return True


def test_update_and_read_giveback_ema():
    r = FakeRedis()
    cfg = GivebackEmaConfig(enabled=True, alpha=0.5, min_samples_for_use=0, use_regime_dim=True, ttl_sec=60)
    now_ms = get_ny_time_millis()

    # notional=1000, giveback=2 => 2/1000*10000 = 20 bps
    update_giveback_ema(
        r,
        cfg=cfg,
        kind="breakout",
        symbol="BTCUSDT",
        tf="1m",
        regime="na",
        now_ms=now_ms,
        giveback_pnl=2.0,
        entry_price=100.0,
        qty=10.0,
        notional=1000.0,
    )

    st = read_giveback_ema(r, cfg=cfg, kind="breakout", symbol="BTCUSDT", tf="1m", regime="na")
    assert st is not None
    assert st["samples"] == 1
    assert abs(st["ema_giveback_bps"] - 20.0) < 1e-6
