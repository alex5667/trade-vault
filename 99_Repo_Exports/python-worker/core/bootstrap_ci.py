"""bootstrap_ci.py — Bootstrap confidence intervals for AB winner evaluation.

Pure Python (no numpy). Deterministic via seeded Random.
Used by tools/meta_ab_winner_evaluator_v2.py (Stage 4 AB gating).

Key functions:
  bootstrap_mean_diff(a, b)  — CI for mean(a) - mean(b)   (expR delta)
  bootstrap_rate_diff(a01, b01) — CI for rate diff on 0/1 vectors (tail delta)

Design:
  - Sampling is within-group with replacement (standard non-parametric bootstrap)
  - alpha=0.05 → 95% CI by default (nearest-rank quantile on sorted bootstrap diffs)
  - seed ensures reproducibility for the same dataset + config
"""
from __future__ import annotations

from dataclasses import dataclass
from random import Random
from typing import List


@dataclass(frozen=True)
class BootstrapCI:
    """Result of a single bootstrap CI computation.

    Fields:
      mean   — observed point difference (challenger − champion)
      lo     — lower bound of (1-alpha) CI
      hi     — upper bound of (1-alpha) CI
      n      — total sample size (n_a + n_b)
      n_boot — number of bootstrap iterations actually used
      seed   — RNG seed (for reproducibility audit)
      alpha  — significance level (e.g. 0.05 → 95% CI)
    """

    mean: float
    lo: float
    hi: float
    n: int
    n_boot: int
    seed: int
    alpha: float


def _quantile_sorted(xs_sorted: List[float], q: float) -> float:
    """Nearest-rank quantile on a pre-sorted list.

    Returns NaN for empty input, first/last element for q≤0 or q≥1.
    """
    if not xs_sorted:
        return float("nan")
    if q <= 0.0:
        return xs_sorted[0]
    if q >= 1.0:
        return xs_sorted[-1]
    # nearest-rank on index
    idx = int(round(q * (len(xs_sorted) - 1)))
    if idx < 0:
        idx = 0
    if idx >= len(xs_sorted):
        idx = len(xs_sorted) - 1
    return xs_sorted[idx]


def bootstrap_mean_diff(
    a: List[float],
    b: List[float],
    *,
    n_boot: int = 400,
    alpha: float = 0.05,
    seed: int = 7,
) -> BootstrapCI:
    """Bootstrap CI for mean(a) - mean(b).

    Sampling is within-group with replacement.

    Args:
        a: challenger metric values (e.g. contrib per candidate)
        b: champion metric values
        n_boot: number of bootstrap resamples (higher → narrower variance in CI estimate)
        alpha: significance level; 0.05 → 95% CI
        seed: RNG seed for full reproducibility

    Returns:
        BootstrapCI with mean=observed diff, lo/hi=CI bounds
    """
    n_a = len(a)
    n_b = len(b)
    if n_a == 0 or n_b == 0:
        return BootstrapCI(
            float("nan"), float("nan"), float("nan"),
            n_a + n_b, int(n_boot), int(seed), float(alpha),
        )

    obs = (sum(a) / float(n_a)) - (sum(b) / float(n_b))
    rng = Random(int(seed))
    diffs: List[float] = []
    n_boot_i = max(1, int(n_boot))
    for _ in range(n_boot_i):
        sa = 0.0
        sb = 0.0
        for _i in range(n_a):
            sa += a[rng.randrange(n_a)]
        for _j in range(n_b):
            sb += b[rng.randrange(n_b)]
        diffs.append((sa / float(n_a)) - (sb / float(n_b)))
    diffs.sort()
    lo = _quantile_sorted(diffs, float(alpha) / 2.0)
    hi = _quantile_sorted(diffs, 1.0 - float(alpha) / 2.0)
    return BootstrapCI(float(obs), float(lo), float(hi), n_a + n_b, n_boot_i, int(seed), float(alpha))


def bootstrap_rate_diff(
    a01: List[int],
    b01: List[int],
    *,
    n_boot: int = 400,
    alpha: float = 0.05,
    seed: int = 7,
) -> BootstrapCI:
    """Bootstrap CI for rate(a) - rate(b), where a/b are 0/1 indicator lists.

    Thin wrapper over bootstrap_mean_diff: 0/1 rate is the mean of 0/1 values.

    Typical usage: tail-loss-rate delta (challenger tail − champion tail).
    """
    # Treat as mean of 0/1
    a = [float(x) for x in a01]
    b = [float(x) for x in b01]
    return bootstrap_mean_diff(a, b, n_boot=n_boot, alpha=alpha, seed=seed)
