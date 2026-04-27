from __future__ import annotations

"""Two-sample KS helpers for nightly feature-drift analysis.

We intentionally implement a small dependency-light approximation instead of
requiring scipy in production timers/exporters.
"""

import math
from dataclasses import dataclass
from typing import Iterable, List, Sequence

import numpy as np

# ---------------------------------------------------------------------------
# GPU / CPU array backend (CuPy preferred, numpy fallback)
# ---------------------------------------------------------------------------
try:
    import cupy as cp  # type: ignore[import-untyped]
    _GPU = True
except ImportError:
    cp = None
    _GPU = False


@dataclass(frozen=True)
class KsResult:
    ks_stat: float
    ks_pvalue: float
    n_ref: int
    n_cur: int


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
    return np.asarray(vals, dtype=np.float64)


def ks_statistic(ref: Sequence[float | int | None], cur: Sequence[float | int | None]) -> float:
    """Two-sample KS statistic.

    GPU-accelerated: uses CuPy radix sort for arrays ≥5000 elements.
    Fallback: numpy for smaller arrays or when CuPy is unavailable.
    """
    x_raw = _as_float_array(ref)
    y_raw = _as_float_array(cur)
    nx = int(x_raw.size)
    ny = int(y_raw.size)
    if nx <= 0 or ny <= 0:
        return 0.0

    # GPU path: sort + searchsorted on GPU for large arrays
    if _GPU and cp is not None and (nx + ny) >= 5000:
        try:
            x_gpu = cp.sort(cp.asarray(x_raw))
            y_gpu = cp.sort(cp.asarray(y_raw))
            values_gpu = cp.concatenate([x_gpu, y_gpu])
            cdf_x = cp.searchsorted(x_gpu, values_gpu, side="right").astype(cp.float64) / float(nx)
            cdf_y = cp.searchsorted(y_gpu, values_gpu, side="right").astype(cp.float64) / float(ny)
            return float(cp.max(cp.abs(cdf_x - cdf_y)))
        except Exception:
            pass  # fall through to CPU

    # CPU path
    x = np.sort(x_raw)
    y = np.sort(y_raw)
    values = np.concatenate([x, y])
    cdf_x = np.searchsorted(x, values, side="right") / float(nx)
    cdf_y = np.searchsorted(y, values, side="right") / float(ny)
    return float(np.max(np.abs(cdf_x - cdf_y)))


def _kolmogorov_q(lmbd: float) -> float:
    """Asymptotic survival function for Kolmogorov distribution.

    Good enough for batch drift triage; not intended for scientific publication.
    """
    if lmbd <= 0.0:
        return 1.0
    s = 0.0
    # 100 terms is plenty for our value range.
    for k in range(1, 101):
        term = (-1.0) ** (k - 1) * math.exp(-2.0 * (k * k) * (lmbd * lmbd))
        s += term
        if abs(term) < 1e-12:
            break
    return float(max(0.0, min(1.0, 2.0 * s)))


def ks_pvalue_from_stat(ks_stat: float, n_ref: int, n_cur: int) -> float:
    if ks_stat <= 0.0:
        return 1.0
    if n_ref <= 0 or n_cur <= 0:
        return 1.0
    n_eff = float(n_ref * n_cur) / float(n_ref + n_cur)
    lmbd = (math.sqrt(n_eff) + 0.12 + 0.11 / max(math.sqrt(n_eff), 1e-9)) * float(ks_stat)
    return _kolmogorov_q(lmbd)


def ks_report(ref: Sequence[float | int | None], cur: Sequence[float | int | None]) -> KsResult:
    x = _as_float_array(ref)
    y = _as_float_array(cur)
    stat = ks_statistic(x, y)
    pvalue = ks_pvalue_from_stat(stat, int(x.size), int(y.size))
    return KsResult(ks_stat=float(stat), ks_pvalue=float(pvalue), n_ref=int(x.size), n_cur=int(y.size))
