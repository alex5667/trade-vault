"""Tick time policy: detect -> sanitize -> quarantine.

Goals:
- Keep time monotonic for deterministic downstream logic.
- Bound future/past skew using an ingest-time 'now' (prefer tick['written_at']).
- Provide explicit decisions for observability and replay.

This policy is designed to be used inside the hot tick loop (small allocations only).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Tuple, Optional


@dataclass(frozen=True)
class TickTimePolicy:
    # If tick timestamp is ahead of ingest_now_ms by more than this, hard-drop.
    max_future_ms: int = int(os.getenv("TICK_TIME_MAX_FUTURE_MS", os.getenv("TICK_WATERMARK_FUTURE_MS", "5000")) or 5000)

    # If tick is older than ingest_now_ms by more than this, hard-drop.
    max_past_ms: int = int(os.getenv("TICK_TIME_MAX_PAST_MS", "120000") or 120000)

    # Out-of-order tolerance relative to prev_ts_ms (monotonic watermark).
    # Small back-skews may be clamped forward (soft reorder) to keep monotonic.
    max_reorder_ms: int = int(os.getenv("TICK_TIME_MAX_REORDER_MS", os.getenv("TIME_MAX_BACK_MS", "1500")) or 1500)

    # If True: when tick is in (now .. now+max_future_ms], clamp it to now (or prev+1).
    clamp_soft_future: bool = str(os.getenv("TICK_TIME_CLAMP_SOFT_FUTURE", "1")).lower() in {"1", "true", "yes"}

    # If True: allow soft reorder (tick behind prev within max_reorder_ms) by clamping to prev+1.
    allow_soft_reorder: bool = str(os.getenv("TICK_TIME_ALLOW_SOFT_REORDER", "1")).lower() in {"1", "true", "yes"}


def apply_tick_time_policy(
    *,
    tick_ts_ms: int,
    ingest_now_ms: int,
    prev_ts_ms: int,
    policy: Optional[TickTimePolicy] = None,
) -> Tuple[int, str, Dict[str, int]]:
    """Apply time policy and return (normalized_ts_ms, decision, meta).

    decision values:
      ok | clamp_future | drop_future | drop_past | reorder_soft | reorder_hard | drop_missing

    meta contains small ints: skew_ms/back_ms/age_ms + orig_ts_ms/now_ms/prev_ts_ms + norm_ts_ms (if clamped).
    """
    pol = policy or TickTimePolicy()
    ts = int(tick_ts_ms or 0)
    now = int(ingest_now_ms or 0)
    prev = int(prev_ts_ms or 0)

    meta: Dict[str, int] = {
        "orig_ts_ms": ts,
        "now_ms": now,
        "prev_ts_ms": prev,
    }

    if ts <= 0:
        return 0, "drop_missing", meta

    # If ingest time is missing, fall back to prev (deterministic) and then to ts itself.
    if now <= 0:
        now = prev if prev > 0 else ts
        meta["now_ms"] = now

    # Future guard: clamp within window, drop beyond.
    if ts > now:
        skew = ts - now
        meta["skew_ms"] = int(skew)
        if skew > int(pol.max_future_ms):
            return 0, "drop_future", meta
        if pol.clamp_soft_future:
            ts2 = now
            if prev > 0 and ts2 <= prev:
                ts2 = prev + 1
            meta["norm_ts_ms"] = int(ts2)
            return int(ts2), "clamp_future", meta

    # Past guard (relative to ingest time)
    age = now - ts
    if age > int(pol.max_past_ms):
        meta["age_ms"] = int(age)
        return 0, "drop_past", meta

    # Monotonic / reorder guard (relative to prev watermark)
    if prev > 0 and ts < prev:
        back = prev - ts
        meta["back_ms"] = int(back)
        if back > int(pol.max_reorder_ms):
            return 0, "reorder_hard", meta
        if pol.allow_soft_reorder:
            ts2 = prev + 1
            meta["norm_ts_ms"] = int(ts2)
            return int(ts2), "reorder_soft", meta
        return 0, "reorder_hard", meta

    return int(ts), "ok", meta

