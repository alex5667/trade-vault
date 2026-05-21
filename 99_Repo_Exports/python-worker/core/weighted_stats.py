"""Shared weighted-statistics primitives used by IPS-style calibrators.

Extracted from `core.p_edge_threshold_calibrator` so other calibrators
(adverse_cross, smt_coh, reliability, …) can reuse the same Type-7 quantile
math without duplicating it.

See ``core/reject_reason_weights.py`` for the reason→weight policy that
produces the weight values fed into these primitives.
"""
from __future__ import annotations

import math


def weighted_quantile(xs_w: list[tuple[float, float]], q: float) -> float:
    """Weighted linear-interpolated quantile (Type-7, R/numpy default).

    Treats each sample as occupying `w` units on a virtual rank axis. For
    uniform weight=1.0 the result is bit-for-bit identical to a Type-7
    unweighted quantile. Zero-weight samples are skipped. Returns 0.0 on
    empty input.

    Args:
        xs_w: list of (value, weight) pairs. Weights must be ≥ 0; negative
              or non-finite weights are silently skipped.
        q:    quantile ∈ [0, 1] (clamped to [0, 0.999]).
    """
    if not xs_w:
        return 0.0
    pairs = sorted(
        ((v, w) for v, w in xs_w if math.isfinite(w) and w > 0.0),
        key=lambda t: t[0],
    )
    if not pairs:
        return 0.0
    if len(pairs) == 1:
        return pairs[0][0]
    total_w = sum(w for _v, w in pairs)
    if total_w <= 1.0:
        # Insufficient total mass for type-7 interpolation; pick the median
        # value by mass.
        half = total_w / 2.0
        c = 0.0
        for v, w in pairs:
            c += w
            if c >= half:
                return v
        return pairs[-1][0]
    q = min(0.999, max(0.0, q))
    target = q * (total_w - 1.0)
    cum = 0.0  # cumulative weight EXCLUSIVE of the current sample
    prev_pos = 0.0
    prev_v: float = pairs[0][0]
    for v, w in pairs:
        if cum >= target:
            span = cum - prev_pos
            if span <= 0.0:
                return v
            frac = (target - prev_pos) / span
            return prev_v + (v - prev_v) * frac
        prev_pos = cum
        prev_v = v
        cum += w
    return pairs[-1][0]


def weighted_mean(xs_w: list[tuple[float, float]]) -> float:
    """Σ(v·w) / Σw. Returns 0.0 on empty / non-positive total weight."""
    total = 0.0
    sum_vw = 0.0
    for v, w in xs_w:
        if not math.isfinite(w) or w <= 0.0:
            continue
        total += w
        sum_vw += v * w
    if total <= 0.0:
        return 0.0
    return sum_vw / total


def effective_n(xs_w: list[tuple[float, float]]) -> float:
    """Σw — effective sample count when each sample contributes `w` units."""
    return sum(w for _v, w in xs_w if math.isfinite(w) and w > 0.0)
