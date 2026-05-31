"""Bootstrap confidence intervals for avg_r_lift.

Deterministic given (seed, n_boot, inputs) — required for replay validity.
"""

from __future__ import annotations

import random

from services.gate_value_reporter.contracts import ConfidenceInterval

_EMPTY_CI = ConfidenceInterval(0.0, 0.0, 0.0)


def bootstrap_avg_r_lift(
    passed_rs: list[float],
    gated_rs: list[float],
    *,
    n_boot: int = 1000,
    seed: int = 42,
    lo_q: float = 0.05,
    hi_q: float = 0.95,
) -> ConfidenceInterval:
    """Bootstrap CI for (mean(passed_rs) − mean(gated_rs)).

    Returns ConfidenceInterval(lo, mid, hi). Empty inputs → all zeros.
    """
    if not passed_rs or not gated_rs:
        return _EMPTY_CI

    rnd = random.Random(seed)
    p_n = len(passed_rs)
    g_n = len(gated_rs)
    lifts: list[float] = [0.0] * n_boot

    for i in range(n_boot):
        p_sum = 0.0
        for _ in range(p_n):
            p_sum += rnd.choice(passed_rs)
        g_sum = 0.0
        for _ in range(g_n):
            g_sum += rnd.choice(gated_rs)
        lifts[i] = (p_sum / p_n) - (g_sum / g_n)

    lifts.sort()
    lo_idx = max(0, min(n_boot - 1, int(lo_q * n_boot)))
    mid_idx = max(0, min(n_boot - 1, int(0.50 * n_boot)))
    hi_idx = max(0, min(n_boot - 1, int(hi_q * n_boot)))

    return ConfidenceInterval(lo=lifts[lo_idx], mid=lifts[mid_idx], hi=lifts[hi_idx])
