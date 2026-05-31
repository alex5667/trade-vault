from __future__ import annotations

from dataclasses import dataclass
from collections import deque
from typing import Deque, Optional, Tuple
import math


def _pct(xs, p: float) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    if p <= 0:
        return float(ys[0])
    if p >= 100:
        return float(ys[-1])
    k = (len(ys) - 1) * (p / 100.0)
    lo = int(math.floor(k))
    hi = int(math.ceil(k))
    if lo == hi:
        return float(ys[lo])
    a = ys[lo]
    b = ys[hi]
    w = k - lo
    return float(a + (b - a) * w)


@dataclass
class Stability:
    ema: float
    drift: float
    range_norm: float
    median: float
    p95: float
    p05: float
    n: int


class ThresholdStabilityTracker:
    """
    Tracks:
      - EMA(th)
      - drift = |th-ema|/max(|ema|,eps)
      - range_norm = (p95-p05)/max(|median|,eps) on rolling window
    Deterministic, O(1) update + O(W log W) percentile on snapshot (W small).
    """
    def __init__(self, *, alpha: float = 0.05, window: int = 200, eps: float = 1e-12) -> None:
        self.alpha = float(alpha)
        self.window = int(window)
        self.eps = float(eps)
        self._ema: Optional[float] = None
        self._buf: Deque[float] = deque(maxlen=self.window)

    def update(self, x: float) -> Stability:
        v = float(x)
        if not math.isfinite(v) or v <= 0:
            # keep old state
            ema = float(self._ema or 0.0)
            buf = list(self._buf)
            med = _pct(buf, 50) if buf else 0.0
            p95 = _pct(buf, 95) if buf else 0.0
            p05 = _pct(buf, 5) if buf else 0.0
            denom = max(self.eps, abs(med))
            rn = (p95 - p05) / denom if denom > 0 else 0.0
            return Stability(ema=ema, drift=0.0, range_norm=float(rn), median=float(med), p95=float(p95), p05=float(p05), n=len(buf))

        # EMA update
        if self._ema is None:
            self._ema = v
        else:
            self._ema = (1.0 - self.alpha) * float(self._ema) + self.alpha * v

        self._buf.append(v)
        ema = float(self._ema)
        drift = abs(v - ema) / max(self.eps, abs(ema))

        buf = list(self._buf)
        med = _pct(buf, 50)
        p95 = _pct(buf, 95)
        p05 = _pct(buf, 5)
        denom = max(self.eps, abs(med))
        rn = (p95 - p05) / denom if denom > 0 else 0.0

        return Stability(ema=ema, drift=float(drift), range_norm=float(rn), median=float(med), p95=float(p95), p05=float(p05), n=len(buf))
