from __future__ import annotations

from typing import Any, Dict, Optional

from services.ev_tp1_stats import EvTp1StatsConfig, update_tp1_hit_ema


class FakeRedis:
    def __init__(self) -> None:
        self.h: Dict[str, Dict[str, str]] = {}
        self.ttl: Dict[str, int] = {}

    def hincrby(self, key: str, field: str, amount: int) -> int:
        m = self.h.setdefault(key, {})
        v = int(m.get(field, "0"))
        v += int(amount)
        m[field] = str(v)
        return v

    def hget(self, key: str, field: str) -> Optional[str]:
        return self.h.get(key, {}).get(field)

    def hset(self, key: str, mapping: Dict[str, Any]) -> None:
        m = self.h.setdefault(key, {})
        for k, v in mapping.items():
            m[str(k)] = str(v)

    def expire(self, key: str, ttl: int) -> None:
        self.ttl[key] = int(ttl)


def test_tp1_ema_updates_and_counts():
    r = FakeRedis()
    cfg = EvTp1StatsConfig(enabled=True, alpha=0.2, ttl_sec=3600)

    total1, ema1 = update_tp1_hit_ema(
        r, cfg=cfg, kind="CryptoOrderFlow", symbol="BTCUSDT", tf="1m", regime="trend", tp1_hit=1, now_ms=1
    )
    assert total1 == 1
    assert abs(ema1 - 1.0) < 1e-9

    total2, ema2 = update_tp1_hit_ema(
        r, cfg=cfg, kind="CryptoOrderFlow", symbol="BTCUSDT", tf="1m", regime="trend", tp1_hit=0, now_ms=2
    )
    assert total2 == 2
    # ema = 1 + 0.2*(0-1)=0.8
    assert abs(ema2 - 0.8) < 1e-9