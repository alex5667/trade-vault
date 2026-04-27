"""Stateless tick-time policy enforcement for strategy loops."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from common.time_norm import normalize_epoch_ms


TickTimeDecision = str


@dataclass(frozen=True)
class TickTimePolicy:
    max_future_ms: int = 5_000
    max_past_ms: int = 120_000
    max_reorder_ms: int = 1_500
    clamp_soft_future: bool = True
    allow_soft_reorder: bool = True


def _meta(*, raw_ts_ms: int, norm_ts_ms: int, now_ms: int, prev_ts_ms: Optional[int], skew_ms: int, age_ms: int, back_ms: int) -> Dict[str, Any]:
    return {
        "raw_ts_ms": int(raw_ts_ms),
        "norm_ts_ms": int(norm_ts_ms),
        "now_ms": int(now_ms),
        "prev_ts_ms": int(prev_ts_ms) if prev_ts_ms is not None else None,
        "skew_ms": int(skew_ms),
        "age_ms": int(age_ms),
        "back_ms": int(back_ms),
    }


def apply_tick_time_policy(
    tick_ts_ms: Any,
    ingest_now_ms: int,
    prev_ts_ms: Optional[int],
    policy: TickTimePolicy,
) -> Tuple[int, TickTimeDecision, Dict[str, Any]]:
    """Apply policy to a single tick timestamp.

    Returns: (sanitized_ts_ms, decision, meta)

    Decisions:
    - ok
    - clamp_future
    - drop_future
    - drop_past
    - reorder_soft
    - reorder_hard
    - drop_missing

    Notes:
    - This function is stateless. Caller is responsible for tracking prev_ts_ms.
    """

    raw_i = 0
    try:
        raw_i = int(tick_ts_ms) if tick_ts_ms is not None else 0
    except Exception:
        raw_i = 0

    norm = normalize_epoch_ms(raw_i) if raw_i else None
    if norm is None:
        return 0, "drop_missing", {"raw_ts_ms": raw_i, "norm_ts_ms": None, "now_ms": int(ingest_now_ms), "prev_ts_ms": int(prev_ts_ms) if prev_ts_ms is not None else None}

    now_ms = int(ingest_now_ms)
    ts_ms = int(norm)

    skew_ms = int(ts_ms) - int(now_ms)
    age_ms = int(now_ms) - int(ts_ms)
    back_ms = int(prev_ts_ms - ts_ms) if (prev_ts_ms is not None and ts_ms < int(prev_ts_ms)) else 0

    decision: TickTimeDecision = "ok"

    # Future clamp/drop
    if skew_ms > int(policy.max_future_ms):
        if policy.clamp_soft_future:
            decision = "clamp_future"
            ts_ms = int(now_ms)
            skew_ms = 0
            age_ms = 0
            back_ms = int(prev_ts_ms - ts_ms) if (prev_ts_ms is not None and ts_ms < int(prev_ts_ms)) else 0
        else:
            decision = "drop_future"
            return ts_ms, decision, _meta(raw_ts_ms=raw_i, norm_ts_ms=ts_ms, now_ms=now_ms, prev_ts_ms=prev_ts_ms, skew_ms=skew_ms, age_ms=age_ms, back_ms=back_ms)

    # Past drop
    if age_ms > int(policy.max_past_ms):
        decision = "drop_past"
        return ts_ms, decision, _meta(raw_ts_ms=raw_i, norm_ts_ms=ts_ms, now_ms=now_ms, prev_ts_ms=prev_ts_ms, skew_ms=skew_ms, age_ms=age_ms, back_ms=back_ms)

    # Reorder handling (only if prev watermark exists)
    if prev_ts_ms is not None and ts_ms < int(prev_ts_ms):
        if back_ms > int(policy.max_reorder_ms):
            decision = "reorder_hard"
            return ts_ms, decision, _meta(raw_ts_ms=raw_i, norm_ts_ms=ts_ms, now_ms=now_ms, prev_ts_ms=prev_ts_ms, skew_ms=skew_ms, age_ms=age_ms, back_ms=back_ms)
        if policy.allow_soft_reorder:
            decision = "reorder_soft"
            ts_ms = int(prev_ts_ms)
        else:
            decision = "reorder_hard"

    return ts_ms, decision, _meta(raw_ts_ms=raw_i, norm_ts_ms=ts_ms, now_ms=now_ms, prev_ts_ms=prev_ts_ms, skew_ms=skew_ms, age_ms=age_ms, back_ms=back_ms)

