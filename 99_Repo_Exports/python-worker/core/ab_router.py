from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Dict, Tuple


def _sha1_u32(s: str) -> int:
    """Deterministic hash to u32 for consistent arm assignment."""
    h = hashlib.sha1(s.encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big", signed=False)


def choose_arm_abc(*, key: str, split_b: int, split_c: int, salt: str = "") -> str:
    """
    Deterministic A/B/C selection.
      - split_b, split_c are percents (0..100)
      - A gets the remainder: 100 - split_b - split_c

    Example: A=80,B=10,C=10 => split_b=10 split_c=10.
    
    Returns: "A", "B", or "C"
    """
    sb = max(0, min(100, int(split_b)))
    sc = max(0, min(100, int(split_c)))
    if sb + sc >= 100:
        # keep A >= 1%
        sc = max(0, 99 - sb)
    x = _sha1_u32(f"{salt}|{key}") % 100
    if x < sb:
        return "B"
    if x < sb + sc:
        return "C"
    return "A"


@dataclass
class ABSplits:
    """Split configuration for a regime group."""
    b: int = 10
    c: int = 10
    group: str = "default"


def splits_for_regime(*, regime: str, cfg: Dict) -> ABSplits:
    """
    Contextual split by regime group.

    Groups:
      - thin/news/illiquid => "thin" (higher B/C to gather stats faster)
      - else => "default"

    Config keys (ints percents):
      - ab_split_b_default, ab_split_c_default
      - ab_split_b_thin,    ab_split_c_thin
    """
    from contexts import normalize_regime_label, MARKET_REGIME_NA
    rg = normalize_regime_label(regime)
    grp = "thin" if rg in ("thin", "news", "illiquid") else "default"
    if grp == "thin":
        b = int(cfg.get("ab_split_b_thin", cfg.get("ab_split_b_default", 10)))
        c = int(cfg.get("ab_split_c_thin", cfg.get("ab_split_c_default", 10)))
        return ABSplits(b=b, c=c, group="thin")
    b = int(cfg.get("ab_split_b_default", 10))
    c = int(cfg.get("ab_split_c_default", 10))
    return ABSplits(b=b, c=c, group="default")
