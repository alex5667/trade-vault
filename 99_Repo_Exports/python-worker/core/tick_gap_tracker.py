from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field


@dataclass
class TickGapTracker:
    """
    Tracks inter-arrival gaps between ticks (in tick-time ms).

    Hot-path: record() is O(1).
    Percentiles computed on-demand (calibrator interval, not every tick).
    """
    window: int = 512
    last_ts_ms: int | None = None
    gaps_ms: deque[int] = field(default_factory=lambda: deque(maxlen=512))

    def record(self, ts_ms: int) -> None:
        if ts_ms <= 0:
            return
        if self.last_ts_ms is not None and ts_ms > self.last_ts_ms:
            self.gaps_ms.append(int(ts_ms - self.last_ts_ms))
        self.last_ts_ms = int(ts_ms)

    def snapshot(self) -> dict[str, float]:
        xs = list(self.gaps_ms)
        if not xs:
            return {"n": 0.0, "p50": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0}
        xs.sort()
        n = len(xs)
        def q(p: float) -> float:
            if n == 1:
                return float(xs[0])
            i = int(round((n - 1) * p))
            i = max(0, min(n - 1, i))
            return float(xs[i])
        return {
            "n": float(n),
            "p50": q(0.50),
            "p90": q(0.90),
            "p95": q(0.95),
            "p99": q(0.99),
        }
