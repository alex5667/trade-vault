\
from __future__ import annotations

import math
from typing import List, Tuple

def brier_score(y: List[int], p: List[float]) -> float:
    if not y:
        return 0.0
    s = 0.0
    for yi, pi in zip(y, p):
        d = float(pi) - float(yi)
        s += d * d
    return s / float(len(y))

def ece_score(y: List[int], p: List[float], n_bins: int = 10) -> float:
    """
    Expected Calibration Error (ECE).
    """
    if not y:
        return 0.0
    # bins: [0,1]
    bins = [0] * n_bins
    sum_p = [0.0] * n_bins
    sum_y = [0.0] * n_bins
    for yi, pi in zip(y, p):
        pi = max(0.0, min(1.0, float(pi)))
        b = min(n_bins - 1, int(pi * n_bins))
        bins[b] += 1
        sum_p[b] += pi
        sum_y[b] += float(yi)
    ece = 0.0
    n = float(len(y))
    for b in range(n_bins):
        if bins[b] == 0:
            continue
        frac = bins[b] / n
        avg_p = sum_p[b] / bins[b]
        avg_y = sum_y[b] / bins[b]
        ece += frac * abs(avg_p - avg_y)
    return ece

def quantiles(xs: List[float], qs: List[float]) -> List[float]:
    if not xs:
        return [0.0 for _ in qs]
    xs = sorted(xs)
    out = []
    for q in qs:
        i = int(round((len(xs) - 1) * q))
        i = max(0, min(len(xs) - 1, i))
        out.append(float(xs[i]))
    return out

def ks_statistic(a: List[float], b: List[float]) -> float:
    """
    Two-sample KS statistic (simple, no p-value).
    """
    if not a or not b:
        return 0.0
    a = sorted(a)
    b = sorted(b)
    i = j = 0
    na = len(a)
    nb = len(b)
    cdf_a = cdf_b = 0.0
    d = 0.0
    while i < na and j < nb:
        if a[i] <= b[j]:
            i += 1
            cdf_a = i / na
        else:
            j += 1
            cdf_b = j / nb
        d = max(d, abs(cdf_a - cdf_b))
    return d


