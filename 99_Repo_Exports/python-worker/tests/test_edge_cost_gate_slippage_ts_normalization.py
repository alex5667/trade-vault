from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from handlers.crypto_orderflow.utils.edge_cost_gate import estimate_slippage_bps


class RedisSpy:
    def __init__(self) -> None:
        self.calls = 0

    def hgetall(self, key: str) -> dict[str, Any]:
        self.calls += 1
        # even if it returns something, the function must NOT use EMA when ts invalid
        return {"ema": "1.0", "samples": "999"}


@dataclass
class Ctx:
    bid: float = 100.0
    ask: float = 101.0
    tf: str = "1m"
    kind: str = "absorption"
    venue: str = "binance_futures"


def test_ts_seconds_fail_open_no_ema_used() -> None:
    ctx = Ctx()
    r = RedisSpy()
    # ts in SECONDS (10 digits) => treat as suspicious, do NOT use EMA, return max(default, half-spread)
    out = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=1700000000,  # seconds
        kind="absorption",
        default_bps=5.0,
        use_spread_half=True,
    )
    assert r.calls == 0, "EMA read must be skipped for seconds timestamp"
    assert out >= 5.0


def test_ts_invalid_fail_open_no_ema_used() -> None:
    ctx = Ctx()
    r = RedisSpy()
    out = estimate_slippage_bps(
        ctx,
        redis_client=r,
        symbol="BTCUSDT",
        venue="binance_futures",
        ts_ms=0,  # invalid
        kind="absorption",
        default_bps=5.0,
        use_spread_half=True,
    )
    assert r.calls == 0
    assert out >= 5.0
