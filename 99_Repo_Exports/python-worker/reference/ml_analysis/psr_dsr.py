from __future__ import annotations

"""Probabilistic / Deflated Sharpe helpers for nightly strategy research stats.

These helpers intentionally avoid heavy dependencies (scipy/pandas) so they can
run inside the same lightweight timer container as the other research bundles.
The implementation follows the Bailey/Lopez de Prado style formulas closely
enough for rollout gating, while remaining deterministic and numerically safe.
"""

import math
from statistics import NormalDist
from typing import Iterable, List, Sequence


def _clean(values: Iterable[float]) -> List[float]:
    out: List[float] = []
    for v in values:
        try:
            f = float(v)
        except Exception:
            continue
        if math.isfinite(f):
            out.append(f)
    return out


def mean(values: Sequence[float]) -> float:
    xs = _clean(values)
    if not xs:
        return 0.0
    return sum(xs) / float(len(xs))


def sample_variance(values: Sequence[float]) -> float:
    xs = _clean(values)
    n = len(xs)
    if n < 2:
        return 0.0
    mu = mean(xs)
    return sum((x - mu) ** 2 for x in xs) / float(n - 1)


def sample_std(values: Sequence[float]) -> float:
    return math.sqrt(max(sample_variance(values), 0.0))


def skewness(values: Sequence[float]) -> float:
    xs = _clean(values)
    n = len(xs)
    if n < 3:
        return 0.0
    mu = mean(xs)
    s = sample_std(xs)
    if s <= 0.0:
        return 0.0
    m3 = sum(((x - mu) / s) ** 3 for x in xs)
    return (n / ((n - 1) * (n - 2))) * m3


def kurtosis(values: Sequence[float]) -> float:
    """Return Pearson kurtosis (normal == 3)."""
    xs = _clean(values)
    n = len(xs)
    if n < 4:
        return 3.0
    mu = mean(xs)
    s = sample_std(xs)
    if s <= 0.0:
        return 3.0
    z4 = sum(((x - mu) / s) ** 4 for x in xs)
    term1 = (n * (n + 1) * z4) / ((n - 1) * (n - 2) * (n - 3))
    term2 = (3 * (n - 1) ** 2) / ((n - 2) * (n - 3))
    excess = term1 - term2
    return excess + 3.0


def sharpe_ratio(values: Sequence[float], risk_free: float = 0.0) -> float:
    xs = _clean(values)
    if not xs:
        return 0.0
    ex = [x - float(risk_free) for x in xs]
    s = sample_std(ex)
    if s <= 0.0:
        m = mean(ex)
        if m > 0.0:
            return 1e6
        if m < 0.0:
            return -1e6
        return 0.0
    return mean(ex) / s


def probabilistic_sharpe_ratio(values: Sequence[float], benchmark_sr: float = 0.0) -> float:
    """Probability that observed Sharpe exceeds benchmark Sharpe.

    Uses the standard skew/kurtosis-adjusted approximation (Bailey/de Prado).
    """
    xs = _clean(values)
    n = len(xs)
    if n < 2:
        return 0.0
    sr = sharpe_ratio(xs)
    g3 = skewness(xs)
    g4 = kurtosis(xs)
    denom_term = 1.0 - g3 * sr + ((g4 - 1.0) / 4.0) * (sr ** 2)
    denom_term = max(denom_term, 1e-12)
    denom = math.sqrt(denom_term / max(float(n - 1), 1.0))
    if denom <= 0.0:
        return 1.0 if sr > benchmark_sr else 0.0
    z = (sr - float(benchmark_sr)) / denom
    return float(NormalDist().cdf(z))


def expected_max_sharpe_benchmark(n_trials: int, sr_std: float) -> float:
    if n_trials <= 1:
        return 0.0
    # Bailey/de Prado approximation for E[max(SR)] under multiple testing.
    nd = NormalDist()
    gamma = 0.5772156649  # Euler-Mascheroni constant
    a = nd.inv_cdf(1.0 - 1.0 / float(n_trials))
    b = nd.inv_cdf(1.0 - 1.0 / (float(n_trials) * math.e))
    return float(sr_std * ((1.0 - gamma) * a + gamma * b))


def deflated_sharpe_ratio(values: Sequence[float], *, n_trials: int = 1, benchmark_sr: float = 0.0) -> float:
    """Deflated Sharpe Ratio: PSR adjusted for multiple testing bias.

    Args:
        values: sequence of per-period returns/scores
        n_trials: number of strategy variants tested (adjusts benchmark SR upward)
        benchmark_sr: minimum acceptable SR (default 0)

    Returns:
        probability in [0, 1]; higher = better evidence of genuine edge
    """
    xs = _clean(values)
    n = len(xs)
    if n < 2:
        return 0.0
    sr = sharpe_ratio(xs)
    g3 = skewness(xs)
    g4 = kurtosis(xs)
    sr_std = math.sqrt(max(1.0 - g3 * sr + ((g4 - 1.0) / 4.0) * (sr ** 2), 1e-12) / max(float(n - 1), 1.0))
    sr_star = max(float(benchmark_sr), expected_max_sharpe_benchmark(int(max(n_trials, 1)), sr_std))
    return probabilistic_sharpe_ratio(xs, benchmark_sr=sr_star)
