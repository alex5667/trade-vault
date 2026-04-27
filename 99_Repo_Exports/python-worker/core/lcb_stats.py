# -*- coding: utf-8 -*-
"""
LCB (Lower Confidence Bound) utilities.

Goal:
  - pick safer winners with small samples (world practice for online strategy selection)
  - avoid overfitting to short bursts

We use a conservative normal approximation by default:
  LCB = mean - z * (std / sqrt(n))

Notes:
  - For n < 2 -> LCB = -inf (cannot estimate variance)
  - You can tune confidence via z (e.g. 1.28=80%, 1.64=90%, 1.96=95%)
  - Keep it deterministic and dependency-free (no scipy).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple


@dataclass
class LCBStats:
    n: int
    mean: float
    stdev: float
    se: float
    lcb: float


def _mean(xs: List[float]) -> float:
    return float(sum(xs) / max(1, len(xs)))


def _stdev(xs: List[float], m: float) -> float:
    # sample stdev (ddof=1)
    n = len(xs)
    if n < 2:
        return 0.0
    v = 0.0
    for x in xs:
        d = float(x) - float(m)
        v += d * d
    v = v / float(n - 1)
    return float(math.sqrt(max(0.0, v)))


def mean_lcb(
    xs: Iterable[float],
    *,
    z: float = 1.64,          # ~90% one-sided
    min_n: int = 30,
    clamp: Optional[Tuple[float, float]] = None,
) -> LCBStats:
    """
    Compute mean + LCB for values.
    Fail-safe:
      - if not enough samples -> LCB becomes very pessimistic
      - caller should gate by n >= min_n
    """
    arr = [float(x) for x in xs if x is not None and math.isfinite(float(x))]
    n = int(len(arr))
    if n <= 0:
        return LCBStats(n=0, mean=0.0, stdev=0.0, se=0.0, lcb=float("-inf"))
    m = _mean(arr)
    sd = _stdev(arr, m)
    se = float(sd / math.sqrt(n)) if n > 0 else 0.0
    lcb = float(m - float(z) * se) if n >= 2 else float("-inf")
    if clamp is not None:
        lo, hi = clamp
        m = max(lo, min(hi, m))
        if math.isfinite(lcb):
            lcb = max(lo, min(hi, lcb))
    # If under min_n, keep LCB pessimistic so selection won't flip too early.
    if n < int(min_n):
        lcb = float("-inf")
    return LCBStats(n=n, mean=float(m), stdev=float(sd), se=float(se), lcb=float(lcb))
