"""Deterministic canary selector for the confidence meta-gate.

Replay-friendly: a (sid, salt, share) tuple always picks the same bucket and
the same in/out result. Never uses random.* — replay would diverge.
"""
from __future__ import annotations

import hashlib

_CANARY_BUCKETS = 10_000


def canary_bucket(sid: str, salt: str) -> int:
    """Map sid → integer in [0, 10000).

    The salt is part of the hash so changing salts gives a fresh assignment
    without touching any sid values (useful when ramping a new model and
    wanting the canary cohort to rotate).
    """
    raw = f"{salt}:{sid}".encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    return int(digest[:8], 16) % _CANARY_BUCKETS


def is_canary_selected(sid: str, salt: str, share: float) -> bool:
    """Return True iff this sid falls into the canary cohort for the share.

    share is clamped to [0, 1] so an accidental negative or >1 cannot flip
    the selection inside-out.
    """
    if not sid:
        return False
    share = max(0.0, min(1.0, share))
    if share <= 0.0:
        return False
    if share >= 1.0:
        return True
    threshold = int(share * _CANARY_BUCKETS)
    return canary_bucket(sid, salt) < threshold
