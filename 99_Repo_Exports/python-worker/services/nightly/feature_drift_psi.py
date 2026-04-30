from __future__ import annotations

"""Population Stability Index (PSI) helpers for nightly feature-drift analysis.

Design goals
------------
- deterministic, dependency-light implementation
- robust against sparse / zero-inflated features
- explicit support for missing/zero/clip deltas so report consumers can
  distinguish *distribution drift* from *data-quality drift*

References / conventions
------------------------
PSI is computed on reference-vs-current binned distributions:
    PSI = Σ (p_i - q_i) * ln(p_i / q_i)
where p_i is reference share in bin i, q_i is current share in bin i.

We intentionally derive bins from the *reference* window and keep them fixed for
current. That makes the metric interpretable as "how much the current window
moved away from baseline".
"""

import math
from dataclasses import dataclass
from typing import Iterable, List, Sequence, Tuple

import numpy as np


@dataclass(frozen=True)
class PsiResult:
    psi: float
    n_ref: int
    n_cur: int
    missing_rate_ref: float
    missing_rate_cur: float
    missing_rate_delta: float
    zero_rate_ref: float
    zero_rate_cur: float
    zero_rate_delta: float
    clip_rate_ref: float
    clip_rate_cur: float
    clip_rate_delta: float
    clip_lo: float
    clip_hi: float
    bins: int


def _as_float_array(xs: Iterable[float | int | None]) -> np.ndarray:
    vals: List[float] = []
    for x in xs:
        try:
            if x is None:
                continue
            v = float(x)
            if math.isfinite(v):
                vals.append(v)
        except Exception:
            continue
    if not vals:
        return np.asarray([], dtype=np.float64)
    return np.asarray(vals, dtype=np.float64)


def _sanitize_bins(edges: Sequence[float]) -> np.ndarray:
    arr = np.asarray(list(edges), dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return np.asarray([0.0, 1.0], dtype=np.float64)
    arr = np.unique(arr)
    if arr.size < 2:
        x = float(arr[0])
        return np.asarray([x - 1e-9, x + 1e-9], dtype=np.float64)
    return arr


def quantile_bins(
    ref: Sequence[float | int | None]
    *
    n_bins: int = 10
    clip_q_lo: float = 0.005
    clip_q_hi: float = 0.995
) -> Tuple[np.ndarray, float, float]:
    """Build stable reference bins and clip bounds.

    The clip bounds are returned separately because clip deltas are often more
    actionable than PSI itself for heavy-tailed features.
    """
    a = _as_float_array(ref)
    if a.size == 0:
        return np.asarray([0.0, 1.0], dtype=np.float64), 0.0, 0.0

    lo = float(np.quantile(a, clip_q_lo))
    hi = float(np.quantile(a, clip_q_hi))
    if not math.isfinite(lo):
        lo = float(np.min(a))
    if not math.isfinite(hi):
        hi = float(np.max(a))
    if hi < lo:
        lo, hi = hi, lo
    if hi == lo:
        hi = lo + 1e-9

    uniq = np.unique(a)
    if uniq.size > 1 and uniq.size <= max(16, int(n_bins)):
        mids = ((uniq[:-1] + uniq[1:]) / 2.0).astype(np.float64)
        left = float(uniq[0] - max(1e-9, abs(uniq[0]) * 1e-9 + 1e-9))
        right = float(uniq[-1] + max(1e-9, abs(uniq[-1]) * 1e-9 + 1e-9))
        edges = np.concatenate([[left], mids, [right]])
    else:
        qs = np.linspace(0.0, 1.0, max(2, int(n_bins) + 1))
        edges = np.quantile(a, qs)
    edges = _sanitize_bins(edges)
    edges[0] = min(float(edges[0]), lo)
    edges[-1] = max(float(edges[-1]), hi)
    return edges, float(lo), float(hi)


def distribution_from_bins(
    xs: Sequence[float | int | None]
    edges: Sequence[float]
    *
    epsilon: float = 1e-6
) -> np.ndarray:
    a = _as_float_array(xs)
    if a.size == 0:
        # uniform tiny distribution avoids singularities and makes PSI bounded.
        m = max(1, len(edges) - 1)
        return np.full((m,), 1.0 / float(m), dtype=np.float64)

    e = _sanitize_bins(edges)
    counts, _ = np.histogram(a, bins=e)
    probs = counts.astype(np.float64)
    probs = probs + float(epsilon)
    s = float(np.sum(probs))
    if s <= 0:
        return np.full((len(probs),), 1.0 / float(len(probs)), dtype=np.float64)
    return probs / s


def psi_from_distributions(ref_p: Sequence[float], cur_p: Sequence[float], *, epsilon: float = 1e-12) -> float:
    p = np.asarray(ref_p, dtype=np.float64)
    q = np.asarray(cur_p, dtype=np.float64)
    if p.size != q.size:
        raise ValueError("ref/current distributions must have same shape")
    p = np.clip(p, epsilon, None)
    q = np.clip(q, epsilon, None)
    return float(np.sum((p - q) * np.log(p / q)))


def missing_rate(xs: Sequence[float | int | None], *, total_n: int | None = None) -> float:
    if total_n is None:
        total_n = len(list(xs)) if not isinstance(xs, np.ndarray) else int(xs.shape[0])
    if total_n <= 0:
        return 0.0
    miss = 0
    for x in xs:
        try:
            if x is None or not math.isfinite(float(x)):
                miss += 1
        except Exception:
            miss += 1
    return float(miss) / float(total_n)


def zero_rate(xs: Sequence[float | int | None], *, tol: float = 1e-12) -> float:
    n = 0
    z = 0
    for x in xs:
        try:
            v = float(x)
            if not math.isfinite(v):
                continue
            n += 1
            if abs(v) <= tol:
                z += 1
        except Exception:
            continue
    return 0.0 if n <= 0 else float(z) / float(n)


def clip_rate(xs: Sequence[float | int | None], *, lo: float, hi: float) -> float:
    n = 0
    c = 0
    for x in xs:
        try:
            v = float(x)
            if not math.isfinite(v):
                continue
            n += 1
            if v < lo or v > hi:
                c += 1
        except Exception:
            continue
    return 0.0 if n <= 0 else float(c) / float(n)


def psi_report(
    ref: Sequence[float | int | None]
    cur: Sequence[float | int | None]
    *
    n_bins: int = 10
    clip_q_lo: float = 0.005
    clip_q_hi: float = 0.995
    epsilon: float = 1e-6
    total_ref_n: int | None = None
    total_cur_n: int | None = None
) -> PsiResult:
    edges, clip_lo, clip_hi = quantile_bins(ref, n_bins=n_bins, clip_q_lo=clip_q_lo, clip_q_hi=clip_q_hi)
    ref_p = distribution_from_bins(ref, edges, epsilon=epsilon)
    cur_p = distribution_from_bins(cur, edges, epsilon=epsilon)
    psi = psi_from_distributions(ref_p, cur_p, epsilon=epsilon)

    n_ref = len(ref) if total_ref_n is None else int(total_ref_n)
    n_cur = len(cur) if total_cur_n is None else int(total_cur_n)

    mr_ref = missing_rate(ref, total_n=n_ref)
    mr_cur = missing_rate(cur, total_n=n_cur)
    zr_ref = zero_rate(ref)
    zr_cur = zero_rate(cur)
    cr_ref = clip_rate(ref, lo=clip_lo, hi=clip_hi)
    cr_cur = clip_rate(cur, lo=clip_lo, hi=clip_hi)

    return PsiResult(
        psi=float(psi)
        n_ref=int(n_ref)
        n_cur=int(n_cur)
        missing_rate_ref=float(mr_ref)
        missing_rate_cur=float(mr_cur)
        missing_rate_delta=float(mr_cur - mr_ref)
        zero_rate_ref=float(zr_ref)
        zero_rate_cur=float(zr_cur)
        zero_rate_delta=float(zr_cur - zr_ref)
        clip_rate_ref=float(cr_ref)
        clip_rate_cur=float(cr_cur)
        clip_rate_delta=float(cr_cur - cr_ref)
        clip_lo=float(clip_lo)
        clip_hi=float(clip_hi)
        bins=max(1, len(edges) - 1)
    )
