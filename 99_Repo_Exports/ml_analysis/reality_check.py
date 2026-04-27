from __future__ import annotations

"""Universal evaluator helpers for research / promotion gates."""

import math
from typing import Sequence

import numpy as np


def _as_float_array(xs) -> np.ndarray:
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


def net_expectancy(pnl_r: Sequence) -> float:
    """Mean net P&L (expectancy) over a list of trade results."""
    a = _as_float_array(pnl_r)
    return float(np.mean(a)) if a.size else float("nan")


def mean_r(r_multiples: Sequence) -> float:
    """Mean R-multiple (alias of net_expectancy for R-based strategies)."""
    return net_expectancy(r_multiples)


def downside_adjusted_return(returns: Sequence) -> float:
    """Ratio of mean return to downside RMSE (Sortino-style, no annualisation)."""
    a = _as_float_array(returns)
    if a.size == 0:
        return float("nan")
    downside = np.minimum(a, 0.0)
    dd = float(np.sqrt(np.mean(downside ** 2)))
    if dd <= 0.0:
        return float(np.mean(a))
    return float(np.mean(a) / dd)


def precision_at_top_x(labels: Sequence, scores: Sequence, *, x_frac: float = 0.05) -> float:
    """Precision at top x_frac fraction of score-ranked predictions."""
    y = _as_float_array(labels)
    s = _as_float_array(scores)
    n = int(min(y.size, s.size))
    if n <= 0:
        return float("nan")
    y = y[:n]
    s = s[:n]
    k = max(1, int(math.ceil(float(x_frac) * n)))
    idx = np.argsort(-s)[:k]
    return float(np.mean(y[idx] > 0.0))


def hit_rate_conditioned_on_cost(pnl: Sequence, costs_bps: Sequence) -> float:
    """Fraction of trades that are net profitable after subtracting cost."""
    p = _as_float_array(pnl)
    c = _as_float_array(costs_bps)
    n = int(min(p.size, c.size))
    if n <= 0:
        return float("nan")
    p = p[:n]
    c = c[:n]
    return float(np.mean((p - c) > 0.0))
