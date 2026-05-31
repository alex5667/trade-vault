"""Stable enum-only reason codes for the confidence meta-gate.

Never write free-text reasons; downstream metrics, SQL audit and UI all key
on these values. New reasons must be appended to the enum.
"""
from __future__ import annotations

from enum import Enum


class MetaGateReason(str, Enum):
    # ── lifecycle / mode ──────────────────────────────────────────────
    MODE_OFF = "mode_off"
    MODE_LEGACY_ONLY = "mode_legacy_only"
    MODE_SHADOW = "mode_shadow"
    MODE_CANARY_SELECTED = "mode_canary_selected"
    MODE_CANARY_NOT_SELECTED = "mode_canary_not_selected"
    MODE_ENFORCE = "mode_enforce"
    MODE_KILL_SWITCH = "mode_kill_switch"
    LEGACY_FALLBACK = "legacy_fallback"

    # ── model state ───────────────────────────────────────────────────
    MODEL_NOT_LOADED = "model_not_loaded"
    MODEL_STALE = "model_stale"
    MODEL_ERROR = "model_error"
    CALIBRATION_STALE = "calibration_stale"
    CALIBRATION_ECE_HIGH = "calibration_ece_high"
    SCHEMA_MISMATCH = "schema_mismatch"

    # ── probability / edge ────────────────────────────────────────────
    P_WIN_BELOW_FLOOR = "p_win_below_floor"
    EXPECTED_R_BELOW_FLOOR = "expected_r_below_floor"
    EXPECTED_EDGE_BELOW_FLOOR = "expected_edge_below_floor"
    PROBABILITY_OK = "probability_ok"
    EDGE_OK = "edge_ok"

    # ── data quality ──────────────────────────────────────────────────
    DQ_DEGRADED = "dq_degraded"
    SIGNAL_STALE = "signal_stale"
    FEATURE_MISSING = "feature_missing"
    FEATURE_SCHEMA_MISMATCH = "feature_schema_mismatch"

    # ── execution / risk ──────────────────────────────────────────────
    EXEC_COST_HIGH = "exec_cost_high"
    SPREAD_SOFT_CAP = "spread_soft_cap"
    SLIPPAGE_SOFT_CAP = "slippage_soft_cap"

    # ── final ─────────────────────────────────────────────────────────
    META_ALLOW = "meta_allow"
    META_ALLOW_TIGHTENED = "meta_allow_tightened"
    META_DENY_SOFT = "meta_deny_soft"
    META_INCONCLUSIVE = "meta_inconclusive"


# Pre-built set for O(1) validation of incoming reason strings (e.g. from JSON).
_VALID_REASON_VALUES: frozenset[str] = frozenset(r.value for r in MetaGateReason)


def is_valid_reason(value: str) -> bool:
    return value in _VALID_REASON_VALUES
