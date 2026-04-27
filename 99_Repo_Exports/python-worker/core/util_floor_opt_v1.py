from __future__ import annotations

from dataclasses import dataclass
import numpy as np


@dataclass(frozen=True)
class FloorResult:
    """Result of floor optimization: best floor and metrics."""
    floor: float
    n_take: int
    take_rate: float
    mean_util: float
    sum_util: float


def best_floor_by_sum_util(
    *,
    score: np.ndarray,
    util_true: np.ndarray,
    floor_min: float,
    floor_max: float,
    floor_step: float,
    min_trades: int,
) -> FloorResult:
    """Find floor that maximizes sum(util_true) subject to min_trades constraint.
    
    Args:
        score: Predicted scores (higher = better)
        util_true: Realized utility values
        floor_min: Minimum floor to consider
        floor_max: Maximum floor to consider
        floor_step: Step size for grid search
        min_trades: Minimum number of trades required
        
    Returns:
        FloorResult with best floor and metrics
    """
    best = FloorResult(floor=floor_max, n_take=0, take_rate=0.0, mean_util=0.0, sum_util=-1e18)
    n = len(score)
    if n == 0:
        return best

    grid = np.arange(floor_min, floor_max + 1e-12, floor_step, dtype=float)
    for f in grid:
        take = score >= f
        n_take = int(take.sum())
        if n_take < int(min_trades):
            continue
        sum_u = float(util_true[take].sum())
        mean_u = float(util_true[take].mean()) if n_take else 0.0
        take_rate = float(n_take / n)
        # Prefer higher sum_util; if tie, prefer higher mean_util
        if sum_u > best.sum_util + 1e-9 or (abs(sum_u - best.sum_util) <= 1e-9 and mean_u > best.mean_util):
            best = FloorResult(floor=float(f), n_take=n_take, take_rate=take_rate, mean_util=mean_u, sum_util=sum_u)
    return best

