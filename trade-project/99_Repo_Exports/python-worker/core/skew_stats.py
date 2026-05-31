from __future__ import annotations

import math
from typing import NamedTuple


class ProportionSkewResult(NamedTuple):
    z_score: float
    p_value: float
    drift_score: float  # Absolute difference
    is_significant: bool


def normal_cdf(x: float) -> float:
    """Standard normal cumulative distribution function (CDF).
    Using math.erf for high precision without scipy.
    """
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


def calculate_proportion_skew(
    train_n: int,
    train_p: float,
    serve_n: int,
    serve_p: float,
    alpha: float = 0.01
) -> ProportionSkewResult:
    """Compare two proportions using Z-test.
    
    Args:
        train_n: Number of samples in training (offline)
        train_p: Proportion of '1' in training (0..1)
        serve_n: Number of samples in serving (online)
        serve_p: Proportion of '1' in serving (0..1)
        alpha: Significance level (default 1%)
        
    Returns:
        ProportionSkewResult with z_score and p_value.
    """
    if train_n <= 0 or serve_n <= 0:
        return ProportionSkewResult(0.0, 1.0, abs(train_p - serve_p), False)

    # Pooled proportion
    p_pool = (train_p * train_n + serve_p * serve_n) / (train_n + serve_n)

    # Standard Error (SE) for pooled proportion
    if p_pool <= 0 or p_pool >= 1:
        # If both are 0 or both are 1, there is no variance, hence no statistical significance
        return ProportionSkewResult(0.0, 1.0, abs(train_p - serve_p), False)

    se = math.sqrt(p_pool * (1 - p_pool) * (1 / train_n + 1 / serve_n))

    # Z-score: difference divided by standard error
    z = (serve_p - train_p) / se

    # Two-tailed P-value
    p_value = 2 * (1 - normal_cdf(abs(z)))

    drift = abs(serve_p - train_p)
    significant = p_value < alpha

    return ProportionSkewResult(z, p_value, drift, significant)
