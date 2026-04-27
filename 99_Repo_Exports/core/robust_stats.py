from __future__ import annotations

from dataclasses import dataclass, field
from collections import deque
from typing import Deque, Optional, List
import math


def _median(xs: List[float]) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    n = len(ys)
    m = n // 2
    if n % 2 == 1:
        return float(ys[m])
    return 0.5 * (float(ys[m - 1]) + float(ys[m]))


@dataclass
class RollingRobustZ:
    """
    Rolling robust z-score using median/MAD.
    z = (x - median) / (1.4826 * MAD + eps)
    Deterministic, O(N log N) on snapshot (N small, e.g. 300).
    """
    window: int = 300
    eps: float = 1e-12
    buf: Deque[float] = field(default_factory=lambda: deque(maxlen=300))

    def __post_init__(self) -> None:
        self.buf = deque(self.buf, maxlen=max(8, int(self.window)))

    def update(self, x: float) -> None:
        if not math.isfinite(x):
            return
        self.buf.append(float(x))

    def median_mad(self) -> tuple[float, float, int]:
        xs = list(self.buf)
        n = len(xs)
        if n < 8:
            return 0.0, 0.0, n
        med = _median(xs)
        dev = [abs(v - med) for v in xs]
        mad = _median(dev)
        return float(med), float(mad), int(n)

    def z(self, x: float) -> float:
        if not math.isfinite(x):
            return 0.0
            
        # УДАЛЕНО: Блок GPU Optimization. 
        # Причина: Трансфер 1000-5000 float-чисел через PCIe шину 
        # многократно медленнее, чем расчет на CPU в L1 кеше.
        
        med, mad, n = self.median_mad()
        if n < 8:
            return 0.0
        denom = 1.4826 * float(mad) + float(self.eps)
        if denom == 0:
            return 0.0
        return float((float(x) - float(med)) / denom)
