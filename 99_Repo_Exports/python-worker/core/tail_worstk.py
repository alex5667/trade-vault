from __future__ import annotations

import heapq
import math
from dataclasses import dataclass
from typing import Any


@dataclass
class WorstK:
    """
    Keep worst-K (smallest) values of a stream.
    Implementation:
      - store negatives in a min-heap -> top is largest x among kept values.
      - O(logK) updates, small K (default 200).
    Supports:
      - mean/std for kept tail
      - export/import for persistence in Redis
    """
    k: int = 200
    _h: list[float] = None  # heap of (-x)
    _sum: float = 0.0
    _sumsq: float = 0.0

    def __post_init__(self) -> None:
        if self._h is None:
            self._h = []

    def n(self) -> int:
        return len(self._h)

    def push(self, x: float) -> None:
        if self.k <= 0 or not math.isfinite(x):
            return
        heapq.heappush(self._h, -float(x))
        self._sum += float(x)
        self._sumsq += float(x) * float(x)
        if len(self._h) > int(self.k):
            # pop largest x among kept -> smallest (-x)
            rm = -heapq.heappop(self._h)
            self._sum -= float(rm)
            self._sumsq -= float(rm) * float(rm)

    def mean_std(self) -> tuple[float, float]:
        n = len(self._h)
        if n <= 0:
            return 0.0, 0.0
        mu = self._sum / float(n)
        var = (self._sumsq / float(n)) - mu * mu
        if var < 0.0:
            var = 0.0
        return float(mu), float(var ** 0.5)

    def to_dict(self) -> dict[str, Any]:
        # store actual x values for deterministic reload (K small)
        xs = [-v for v in self._h]
        return {"k": int(self.k), "xs": xs}

    @staticmethod
    def from_dict(d: dict[str, Any]) -> WorstK:
        k = int(d.get("k", 200) or 200)
        xs = d.get("xs", [])
        w = WorstK(k=k)
        if isinstance(xs, list):
            for x in xs:
                try:
                    w.push(float(x))
                except Exception:
                    continue
        return w
