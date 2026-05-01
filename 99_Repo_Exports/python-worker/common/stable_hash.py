# python-worker/common/stable_hash.py
from __future__ import annotations
"""
Deterministic, cross-process stable hash utilities.

Why:
- Python's built-in `hash()` is randomized per process (PYTHONHASHSEED).
- For sampling / bucketing we need a stable hash.

We use FNV-1a 64-bit (fast, simple, stable across languages if needed).
"""

from typing import Any

_FNV64_OFFSET = 1469598103934665603
_FNV64_PRIME = 1099511628211
_MASK64 = (1 << 64) - 1


def stable_hash64(*parts: Any) -> int:
    """
    Stable 64-bit hash of arbitrary parts.

    Parts are concatenated with a separator to reduce accidental collisions.
    """
    h = _FNV64_OFFSET
    for i, p in enumerate(parts):
        if i:
            # separator
            h ^= 0x1F
            h = (h * _FNV64_PRIME) & _MASK64
        bs = str(p).encode("utf-8", errors="ignore")
        for b in bs:
            h ^= b
            h = (h * _FNV64_PRIME) & _MASK64
    return int(h)


def sample_pct(*parts: Any, pct: int = 1) -> bool:
    """
    Deterministic sampling by percent.

    pct=1 means ~1% of items.
    """
    if pct <= 0:
        return False
    if pct >= 100:
        return True
    return (stable_hash64(*parts) % 100) < pct
