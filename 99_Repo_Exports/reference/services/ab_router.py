from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class Split:
    """Weighted split for arms.
    Example: {"A": 0.8, "B": 0.1, "C": 0.1}
    """
    w: Dict[str, float]

    def normalized(self) -> "Split":
        s = sum(max(0.0, float(v)) for v in self.w.values())
        if s <= 0:
            return Split({"A": 1.0})
        return Split({k: max(0.0, float(v)) / s for k, v in self.w.items()})


def parse_split(spec: str) -> Split:
    """Parse 'A:0.8,B:0.1,C:0.1' into Split."""
    out: Dict[str, float] = {}
    for part in (spec or "").split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            # allow bare arm => weight 1
            out[part.strip().upper()] = 1.0
            continue
        a, b = part.split(":", 1)
        arm = a.strip().upper()
        try:
            w = float(b.strip())
        except Exception:
            w = 0.0
        if arm:
            out[arm] = w
    if not out:
        out = {"A": 1.0}
    return Split(out).normalized()


def _u64_hash(s: str) -> int:
    # Deterministic across processes / machines
    h = hashlib.sha1(s.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big", signed=False)


def choose_arm(*, key: str, split: Split, salt: str = "") -> str:
    """Deterministic weighted choice based on SHA1(key+salt)."""
    sp = split.normalized()
    u = _u64_hash(f"{salt}|{key}") / float(2**64 - 1)
    acc = 0.0
    # Stable iteration order
    for arm in sorted(sp.w.keys()):
        acc += float(sp.w[arm])
        if u <= acc:
            return arm
    # Fallback (precision)
    return sorted(sp.w.keys())[-1]


def split_for_regime(regime: str) -> Split:
    """Contextual split by regime (thin gets more exploration by default)."""
    reg = (regime or "na").strip().lower()
    default = os.getenv("AB_SPLIT_DEFAULT", "A:0.8,B:0.1,C:0.1")
    s_range = os.getenv("AB_SPLIT_RANGE", default)
    s_trend = os.getenv("AB_SPLIT_TREND", default)
    s_thin = os.getenv("AB_SPLIT_THIN", "A:0.6,B:0.2,C:0.2")
    s_news = os.getenv("AB_SPLIT_NEWS", s_thin)

    if reg in ("range",):
        return parse_split(s_range)
    if reg in ("trend", "trending_bull", "trending_bear"):
        return parse_split(s_trend)
    if reg in ("thin", "illiquid"):
        return parse_split(s_thin)
    if reg in ("news",):
        return parse_split(s_news)
    return parse_split(default)


def is_shadow_arm(arm: str) -> bool:
    shadows = os.getenv("AB_SHADOW_ARMS", "B,C")
    s = {x.strip().upper() for x in shadows.split(",") if x.strip()}
    return arm.strip().upper() in s


def salt() -> str:
    return os.getenv("AB_SALT", "smt-entry-v1")
