# -*- coding: utf-8 -*-
"""
ATR floor tier selection (deterministic)
Fixes the "broken chain": floors(t0/t1/t2) must be mapped to atr_bps_th by regime.
"""

from __future__ import annotations

from typing import Any, Dict, Tuple


def compute_atr_bps_threshold(
    *,
    regime: str,
    cfg: Dict[str, Any],
    t0: float,
    t1: float,
    t2: float,
) -> Tuple[int, str, float]:
    rg = str(regime or "na").lower()

    tier = int(cfg.get("atr_floor_tier_default", 1))
    if rg in ("trend", "trending_bull", "trending_bear"):
        tier = int(cfg.get("atr_floor_tier_trend", 0))
    elif rg in ("range", "mixed"):
        tier = int(cfg.get("atr_floor_tier_range", 1))
    elif rg in ("thin", "news", "illiquid"):
        tier = int(cfg.get("atr_floor_tier_thin", 2))

    picked = float(t1 or 0.0)
    if tier <= 0:
        picked = float(t0 or 0.0)
    elif tier >= 2:
        picked = float(t2 or 0.0)

    static_min = float(cfg.get("atr_bps_min_static", 0.0) or 0.0)
    th = float(max(static_min, picked)) if picked > 0 else float(static_min)
    return int(tier), rg, float(th)
