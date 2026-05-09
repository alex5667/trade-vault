from __future__ import annotations

import os
from collections import defaultdict
from typing import Any

from services.stats_aggregator import _post_applied_hooks


class FakeRedis:
    def __init__(self) -> None:
        self.h: dict[str, dict[str, int]] = defaultdict(dict)

    def pipeline(self, transaction: bool = False) -> FakeRedis:
        return self

    def hincrby(self, key: str, field: str, amount: int) -> int:
        cur = int(self.h[key].get(field, 0) or 0)
        cur += int(amount)
        self.h[key][field] = cur
        return cur

    def hset(self, key: str, field: str, value: Any) -> None:
        try:
            self.h[key][field] = int(value)
        except Exception:
            self.h[key][field] = 0

    def expire(self, key: str, ttl: int) -> None:
        return None

    def execute(self) -> None:
        return None


def test_post_applied_hooks_writes_default_dual_curves() -> None:
    # Default is tp2,nosl_after_tp1 (pipeline recommendation).
    os.environ["REL_CAL_ENABLED"] = "1"
    os.environ.pop("REL_CAL_OUTCOMES", None)
    os.environ["REL_CAL_BUCKET_STEP_PCT"] = "5"

    r = FakeRedis()
    pos = {
        "strategy": "absorption",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "entry_ts_ms": 1700000000000,
        "signal_payload": {"confidence": 77.0, "venue": "binance_futures"},
        "tp1_hit": True,
        "tp2_hit": True,
    }
    closed = {
        "strategy": "absorption",
        "symbol": "BTCUSDT",
        "tf": "1m",
        "tp1_hit": True,
        "tp2_hit": True,
        "close_reason": "TP3",
        "pnl_net": 10.0,
        "entry_ts_ms": 1700000000000,
    }
    _post_applied_hooks(r, pos, closed)

    # Expect 2 keys: tp2 and nosl_after_tp1
    assert len(r.h.keys()) == 2
    for k, hv in r.h.items():
        assert hv.get("samples_total", 0) == 1
        assert hv.get("b75:n", 0) == 1  # 77 -> 75 bucket
