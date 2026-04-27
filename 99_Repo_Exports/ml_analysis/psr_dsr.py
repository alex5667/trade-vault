from __future__ import annotations

"""Probabilistic / Deflated Sharpe helpers.

Dependency-light implementation suitable for nightly research jobs.
The formulas follow the Bailey/de Prado style approximation and intentionally
stay deterministic / fail-open for sparse inputs.
"""

import math
from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(float(x) / math.sqrt(2.0)))


def _as_returns(xs: Iterable) -> np.ndarray:
    vals = []
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


def sample_moments(xs: Sequence) -> dict:
    a = _as_returns(xs)
    n = int(a.size)
    if n <= 1:
        return {"n": n, "mean": float("nan"), "std": float("nan"), "skew": 0.0, "kurtosis": 3.0, "sharpe": float("nan")}
    mean = float(np.mean(a))
    std = float(np.std(a, ddof=1))
    if not math.isfinite(std) or std <= 0.0:
        sr = 0.0
    else:
        sr = float(mean / std)
    centered = a - mean
    m2 = float(np.mean(centered ** 2)) if n > 0 else 0.0
    if m2 <= 0.0:
        skew = 0.0
        kurt = 3.0
    else:
        m3 = float(np.mean(centered ** 3))
        m4 = float(np.mean(centered ** 4))
        skew = float(m3 / (m2 ** 1.5)) if m2 > 0 else 0.0
        kurt = float(m4 / (m2 ** 2)) if m2 > 0 else 3.0
    return {"n": n, "mean": mean, "std": std, "skew": skew, "kurtosis": kurt, "sharpe": sr}


def probabilistic_sharpe_ratio(*, observed_sr: float, benchmark_sr: float = 0.0, n: int, skew: float = 0.0, kurtosis: float = 3.0) -> float:
    n_i = int(n)
    if n_i <= 1 or not math.isfinite(observed_sr):
        return float("nan")
    denom = 1.0 - float(skew) * float(observed_sr) + ((float(kurtosis) - 1.0) / 4.0) * (float(observed_sr) ** 2)
    denom = max(denom, 1e-12)
    z = ((float(observed_sr) - float(benchmark_sr)) * math.sqrt(max(n_i - 1, 1))) / math.sqrt(denom)
    return float(_norm_cdf(z))


def deflated_sharpe_ratio(*, observed_sr: float, n: int, trials: int, skew: float = 0.0, kurtosis: float = 3.0) -> float:
    """DSR using a deterministic extreme-value approximation for the expected max SR."""
    m = max(1, int(trials))
    if m == 1:
        benchmark = 0.0
    else:
        # Extreme-value approximation for max of standard normal (Bailey/de Prado).
        benchmark = math.sqrt(2.0 * math.log(m)) - (math.log(math.log(m)) + math.log(4.0 * math.pi)) / (2.0 * math.sqrt(2.0 * math.log(m)))
    return probabilistic_sharpe_ratio(
        observed_sr=float(observed_sr),
        benchmark_sr=float(benchmark),
        n=int(n),
        skew=float(skew),
        kurtosis=float(kurtosis),
    )


@dataclass(frozen=True)
class SharpeReport:
    n: int
    mean: float
    std: float
    sharpe: float
    skew: float
    kurtosis: float
    psr: float
    dsr: float


def sharpe_report(xs: Sequence, *, benchmark_sr: float = 0.0, trials: int = 1) -> SharpeReport:
    mom = sample_moments(xs)
    n = int(mom["n"])
    sr = float(mom["sharpe"])
    psr = probabilistic_sharpe_ratio(
        observed_sr=sr,
        benchmark_sr=float(benchmark_sr),
        n=n,
        skew=float(mom["skew"]),
        kurtosis=float(mom["kurtosis"]),
    )
    dsr = deflated_sharpe_ratio(
        observed_sr=sr,
        n=n,
        trials=max(1, int(trials)),
        skew=float(mom["skew"]),
        kurtosis=float(mom["kurtosis"]),
    )
    return SharpeReport(
        n=n,
        mean=float(mom["mean"]),
        std=float(mom["std"]),
        sharpe=sr,
        skew=float(mom["skew"]),
        kurtosis=float(mom["kurtosis"]),
        psr=float(psr),
        dsr=float(dsr),
    )
