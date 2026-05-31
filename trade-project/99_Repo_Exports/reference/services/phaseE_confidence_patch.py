from __future__ import annotations

"""Phase E: confidence scaling helpers.

Intent
------
You already have ConfidenceScorer with bounded bonuses and caps.
Phase E introduces *quality* dimensions:
- OBI stability score (persistence quality) should scale the OBI bonus.
- fp_edge_absorb strength may optionally scale its bonus.

This module is additive: import from services/signal_confidence.py and apply
at the point where you add the corresponding bonuses.

Design choices
--------------
- clamp quality into [0,1]
- apply a floor so that a stable confirmation is not fully zeroed out by
  a marginal score (avoid oscillation near threshold). Default floor=0.35

You can adjust floor/shape per symbol via config/env later.
"""

from typing import Any


def clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def scale_bonus_by_quality(
    *,
    base_bonus: float,
    quality: float,
    floor: float = 0.35,
) -> float:
    """Scale a bonus by a quality score.

    Example:
        base_bonus = 0.04
        quality = 0.92
        -> 0.04 * max(0.35, 0.92)

    floor prevents "bonus disappears" when quality is noisy near zero.
    """
    q = clamp01(float(quality or 0.0))
    f = clamp01(float(floor or 0.0))
    return float(base_bonus) * max(f, q)


def get_ctx_attr(ctx: Any, name: str, default: float = 0.0) -> float:
    try:
        v = getattr(ctx, name)
        if v is None:
            return float(default)
        return float(v)
    except Exception:
        return float(default)
