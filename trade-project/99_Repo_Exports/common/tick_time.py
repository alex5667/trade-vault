"""Tick timestamp normalization and ordering policy.

This module is intentionally dependency-light so it can be imported by hot paths.

Goals:
- Detect and handle "bad time" ticks: missing/non-epoch, too-far future, too-old past.
- Enforce monotonic-ish time per symbol (soft clamp small reorders, hard drop large).
- Provide structured metadata for observability.

Time format:
- Input can be epoch seconds or epoch milliseconds; normalize to epoch milliseconds.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from common.time_norm import normalize_epoch_ms


TickTimeDecision = str


@dataclass(frozen=True)
class TickTimePolicy:
    max_future_ms: int = 5_000
    max_past_ms: int = 120_000
    max_reorder_ms: int = 1_500
    clamp_soft_future: bool = True
    allow_soft_reorder: bool = True


@dataclass
class SanitizeResult:
    ts_ms: int
    raw_ts_ms: Optional[int] = None
    drop_reason: Optional[str] = None
    flags: List[str] = field(default_factory=list)
    # Helpful diagnostics
    now_ms: Optional[int] = None
    skew_ms: Optional[int] = None
    age_ms: Optional[int] = None
    back_ms: Optional[int] = None

    def to_meta(self) -> Dict[str, Any]:
        return {
            "ts_ms": int(self.ts_ms),
            "raw_ts_ms": int(self.raw_ts_ms) if self.raw_ts_ms is not None else None,
            "drop_reason": self.drop_reason,
            "flags": list(self.flags),
            "now_ms": int(self.now_ms) if self.now_ms is not None else None,
            "skew_ms": int(self.skew_ms) if self.skew_ms is not None else None,
            "age_ms": int(self.age_ms) if self.age_ms is not None else None,
            "back_ms": int(self.back_ms) if self.back_ms is not None else None,
        }


class TickTimeGuard:
    """Stateful guard: keeps a local watermark (last_ts_ms).

    Intended for per-symbol processing loops.
    """

    def __init__(self, policy: TickTimePolicy):
        self.policy = policy
        self.last_ts_ms: Optional[int] = None

    def sanitize_ts_ms(self, ts: Any, *, now_ms: int) -> Optional[SanitizeResult]:
        # Parse/normalize.
        try:
            raw_i = int(ts) if ts is not None else None
        except Exception:
            raw_i = None

        norm = normalize_epoch_ms(raw_i) if raw_i is not None else None
        if norm is None:
            return None

        res = SanitizeResult(ts_ms=int(norm), raw_ts_ms=int(raw_i), now_ms=int(now_ms))

        # Future / past checks
        res.skew_ms = int(res.ts_ms) - int(now_ms)
        res.age_ms = int(now_ms) - int(res.ts_ms)

        if res.skew_ms > int(self.policy.max_future_ms):
            if self.policy.clamp_soft_future:
                res.flags.append("clamp_future")
                res.ts_ms = int(now_ms)
                # Recompute age/skew after clamp
                res.skew_ms = int(res.ts_ms) - int(now_ms)
                res.age_ms = int(now_ms) - int(res.ts_ms)
            else:
                res.drop_reason = "future"
                return res

        if res.age_ms > int(self.policy.max_past_ms):
            res.drop_reason = "past"
            return res

        # Monotonic-ish enforcement per guard instance
        if self.last_ts_ms is not None and int(res.ts_ms) < int(self.last_ts_ms):
            res.back_ms = int(self.last_ts_ms) - int(res.ts_ms)
            if int(res.back_ms) > int(self.policy.max_reorder_ms):
                res.drop_reason = "reorder_hard"
                return res
            if self.policy.allow_soft_reorder:
                res.flags.append("reorder_soft")
                res.ts_ms = int(self.last_ts_ms)
            else:
                res.drop_reason = "reorder_hard"
                return res

        # Update watermark (never go backwards)
        if self.last_ts_ms is None:
            self.last_ts_ms = int(res.ts_ms)
        else:
            self.last_ts_ms = max(int(self.last_ts_ms), int(res.ts_ms))

        return res

