from __future__ import annotations

from dataclasses import dataclass

from common.tick_time import TickTimeGuard, TickTimePolicy


@dataclass
class Tick:
    ts: int
    bid: float = 100.0
    ask: float = 100.2
    last: float = 100.1
    volume: float = 1.0
    flags: int = 1


class MiniMicroFast:
    """
    Minimal behavioral mirror of the hardened preamble inside _update_micro_fast:
      - normalize ts
      - reject future ticks
      - reject too-late ticks when max_tick_lag_ms is set
    """

    def __init__(self, *, now_ms: int, max_tick_lag_ms: int = 0):
        self._now_ms = now_ms
        self.max_tick_lag_ms = max_tick_lag_ms
        self._tick_time = TickTimeGuard(
            TickTimePolicy(max_future_ms=5000, max_past_ms=max(120000, max_tick_lag_ms), max_reorder_ms=1500)
        )
        self.seen: list[int] = []

    def update(self, t: Tick) -> None:
        res = self._tick_time.sanitize_ts_ms(t.ts, now_ms=self._now_ms)
        if res is None or res.drop_reason is not None:
            return
        # past drop (lag)
        if self.max_tick_lag_ms > 0 and (self._now_ms - res.ts_ms) > self.max_tick_lag_ms:
            return
        t.ts = res.ts_ms
        self.seen.append(res.ts_ms)


def test_micro_fast_rejects_future_tick():
    now = 1_700_000_000_000
    m = MiniMicroFast(now_ms=now, max_tick_lag_ms=60_000)
    m.update(Tick(ts=now + 5_001))
    assert m.seen == []


def test_micro_fast_accepts_seconds_ts_and_normalizes():
    now = 1_700_000_000_000
    m = MiniMicroFast(now_ms=now, max_tick_lag_ms=60_000)
    m.update(Tick(ts=1_700_000_000))  # seconds
    assert m.seen == [1_700_000_000_000]


def test_micro_fast_rejects_too_late_tick_by_lag():
    now = 1_700_000_000_000
    m = MiniMicroFast(now_ms=now, max_tick_lag_ms=10_000)
    m.update(Tick(ts=now - 10_001))
    assert m.seen == []
