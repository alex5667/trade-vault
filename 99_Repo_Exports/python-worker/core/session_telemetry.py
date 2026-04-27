from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Tuple, Optional, List

from services.orderflow.utils import hour_of_week_utc, session_utc

def _ema(prev: Optional[float], x: float, alpha: float) -> float:
    return x if prev is None else (1.0 - alpha) * prev + alpha * x

@dataclass
class _HowBucket:
    ema: Optional[float] = None
    n: int = 0

class HourOfWeekScaleTracker:
    def __init__(
        self,
        alpha: float = 0.02,
        clamp_low: float = 0.5,
        clamp_high: float = 2.0,
        min_bucket_n: int = 300,
        min_global_n: int = 2000,
    ):
        self.alpha = float(alpha)
        self.clamp_low = float(clamp_low)
        self.clamp_high = float(clamp_high)
        self.min_bucket_n = int(min_bucket_n)
        self.min_global_n = int(min_global_n)

        self._buckets: List[_HowBucket] = [_HowBucket() for _ in range(168)]
        self._global_ema: Optional[float] = None
        self._global_n: int = 0

    @property
    def global_n(self) -> int:
        return self._global_n

    def bucket_n(self, ts_ms: int) -> int:
        return self._buckets[hour_of_week_utc(ts_ms)].n

    def update(self, ts_ms: int, x: float) -> None:
        if x <= 0:
            return
        how = hour_of_week_utc(ts_ms)
        b = self._buckets[how]
        b.ema = _ema(b.ema, x, self.alpha)
        b.n += 1

        self._global_ema = _ema(self._global_ema, x, self.alpha)
        self._global_n += 1

    def scale(self, ts_ms: int) -> float:
        how = hour_of_week_utc(ts_ms)
        b = self._buckets[how]
        if (
            self._global_n < self.min_global_n
            or b.n < self.min_bucket_n
            or not self._global_ema
            or not b.ema
            or self._global_ema <= 0
            or b.ema <= 0
        ):
            return 1.0
        s = b.ema / self._global_ema
        if s < self.clamp_low:
            return self.clamp_low
        if s > self.clamp_high:
            return self.clamp_high
        return float(s)

class PassRateBySessionEma:
    def __init__(self, alpha: float = 0.05, asia_end_h: int = 8, eu_end_h: int = 16):
        self.alpha = float(alpha)
        self.asia_end_h = int(asia_end_h)
        self.eu_end_h = int(eu_end_h)
        self._ema: Dict[Tuple[str, int], Optional[float]] = {}
        self._n: Dict[Tuple[str, int], int] = {}

    def update(self, ts_ms: int, tier_idx: int, passed: bool) -> float:
        sess = session_utc(ts_ms)
        key = (sess, tier_idx)
        prev = self._ema.get(key)
        self._ema[key] = _ema(prev, 1.0 if passed else 0.0, self.alpha)
        self._n[key] = self._n.get(key, 0) + 1
        return float(self._ema[key] or 0.0)

    def get(self, session: str, tier_idx: int) -> float:
        return float(self._ema.get((session, int(tier_idx))) or 0.0)
