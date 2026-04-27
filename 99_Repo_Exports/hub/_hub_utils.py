# -*- coding: utf-8 -*-
"""
hub/_hub_utils.py — shared utilities for the hub package.

Consolidates code that was previously duplicated between
aggregated_signal_hub.py and aggregated_signal_hub_pro.py.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class HubScore:
    """
    Score produced by any hub variant.

    *metrics* is optional: the base hub omits it; the pro hub populates it
    with z-scores, SVbP values, and detector metadata.
    """
    confidence: float
    dir_up: Optional[bool]
    reason: str
    metrics: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Pivot proximity helper
# ---------------------------------------------------------------------------

_PIVOT_LEVELS = ("P", "R1", "S1", "cam_R3", "cam_S3")


def is_near_pivot(price: float, pivots: dict, atr: float, mult: float = 0.5) -> bool:
    """Return True if *price* is within *mult* × *atr* of any standard pivot level.

    Args:
        price:  Current market price.
        pivots: Dict of pivot level name → value (may contain None entries).
        atr:    Average True Range used as the proximity threshold scale.
        mult:   Multiplier applied to *atr* to form the threshold band.
    """
    if not (price and pivots and atr):
        return False
    thr = atr * mult
    for lvl in _PIVOT_LEVELS:
        v = pivots.get(lvl)
        if not v:
            continue
        if abs(price - float(v)) <= thr:
            return True
    return False


# ---------------------------------------------------------------------------
# Logger factory
# ---------------------------------------------------------------------------

def build_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Return a named logger with a single StreamHandler.

    Idempotent: calling multiple times with the same *name* will not add
    duplicate handlers.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s | %(levelname)5s | %(name)s | %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(level.upper())
    return logger
