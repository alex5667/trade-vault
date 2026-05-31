"""Plan 2 rollout autopilot — shared flag constants and reader functions.

The autopilot writes sticky activation flags + auto-tuned thresholds to a
single Redis HASH. Consumer services (gated_out_outcome_persister,
drift_auto_demote_v1) read this HASH each loop iteration to compute their
effective config: `ENV setting OR autopilot flag`.

Design:
  * Single canonical HASH key `_AUTOPILOT_KEY` — easy ops audit (HGETALL).
  * Flag names are constants here so producers and consumers can't drift.
  * Reader is fail-closed for safety: any Redis error → flag treated as
    not-set (consumers fall back to their ENV setting). Never raises.
  * Sticky activation (HSETNX) on flags S1/S2; per-kind S3 flags can be
    revoked (HDEL) by operator if a kind's auto-demote misfires.
"""
from __future__ import annotations

from typing import Any

# Canonical Redis HASH for Plan 2 rollout state.
AUTOPILOT_KEY = "cfg:autopilot:plan2:state"

# Stage flags (sticky HSETNX; can be HDEL'd manually).
FLAG_PERSISTER_ENABLED      = "gated_out_persister_enabled"        # Stage 1
FLAG_DRIFT_PH_ENABLED       = "drift_page_hinkley_enabled"         # Stage 2
FLAG_AUTO_DEMOTE_PREFIX     = "drift_auto_demote_kind_"            # Stage 3 (per-kind)

# Auto-tuned values (overwrite, not sticky).
FIELD_EXPECTANCY_THRESHOLD  = "expectancy_threshold_tuned"

# Per-flag activation timestamps written alongside (sticky).
def activated_at_field(flag: str) -> str:
    return f"activated_at_{flag}_ms"


def kind_demote_flag(kind: str) -> str:
    """Per-kind auto-demote flag name. Kind is normalized to lowercase."""
    return f"{FLAG_AUTO_DEMOTE_PREFIX}{(kind or '').strip().lower()}"


def read_plan2_flag(rc: Any, flag: str) -> bool:
    """Return True if the given Plan 2 autopilot flag is "1" in Redis.

    Fail-closed: any error → False so consumers default to ENV/shadow.
    """
    try:
        val = rc.hget(AUTOPILOT_KEY, flag)
        return str(val).strip() == "1"
    except Exception:
        return False


def read_plan2_float(rc: Any, field: str, default: float) -> float:
    """Read an auto-tuned float field. Returns `default` on miss/error."""
    try:
        val = rc.hget(AUTOPILOT_KEY, field)
        if val is None:
            return default
        return float(val)
    except (TypeError, ValueError, Exception):
        return default


def is_kind_auto_demote_enabled(rc: Any, kind: str) -> bool:
    """Per-kind allowlist check for Stage 3 (auto-demote)."""
    if not kind:
        return False
    return read_plan2_flag(rc, kind_demote_flag(kind))
