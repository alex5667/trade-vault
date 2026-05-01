from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _ensure_dict(v: Any) -> Dict[str, Any]:
    return dict(v) if isinstance(v, dict) else {}


@dataclass(frozen=True)
class HorizonDQShadow:
    allow_shadow: bool
    shadow_reason_code: str
    atr_selected_value: float
    atr_selected_tf_ms: int
    atr_selected_age_ms: int
    atr_age_budget_ms: int
    book_age_budget_ms: int
    signal_age_budget_ms: int
    selector_reason_code: str
    reason_details: Dict[str, Any]


def compute_horizon_dq_shadow(ctx: Any, cand: Any = None) -> Dict[str, Any]:
    """
    Phase 2.2: shadow-only horizon-aware DQ evaluation.

    Reads horizon fields from ctx and computes horizon-proportional staleness budgets.
    Does NOT enforce any veto by itself — the caller decides via ATR_HORIZON_USE_FOR_GATES.

    Fail-open: any unexpected error returns allow_shadow=True with reason DQ_HZ_INTERNAL_ERROR.

    Returns a dict matching HorizonDQShadow (all fields serialisable).
    """
    try:
        now_ms = int(time.time() * 1000)

        hold_target_ms = _safe_int(
            getattr(ctx, "hold_target_ms", 0)
            or getattr(ctx, "max_hold_target_ms", 0),
            0,
        )
        alpha_half_life_ms = _safe_int(getattr(ctx, "alpha_half_life_ms", 0), 0)
        max_signal_age_ms = _safe_int(getattr(ctx, "max_signal_age_ms", 0), 0)

        # Prefer atr_value / atr — follow same priority as horizon_contract.py
        atr_selected_value = _safe_float(
            getattr(ctx, "atr_value", None)
            or getattr(ctx, "atr", None),
            0.0,
        )
        atr_selected_tf_ms = _safe_int(getattr(ctx, "atr_tf_ms", 0), 0)
        atr_selected_age_ms = _safe_int(getattr(ctx, "atr_age_ms", 0), 0)
        selector_reason_code = str(getattr(ctx, "selector_reason_code", "") or "")

        book_ts_ms = _safe_int(getattr(ctx, "book_ts_ms", 0), 0)
        signal_ts_ms = _safe_int(
            getattr(ctx, "ts", 0) or getattr(ctx, "ts_ms", 0),
            0,
        )
        book_age_ms = max(0, now_ms - book_ts_ms) if book_ts_ms > 0 else 0
        signal_age_ms = max(0, now_ms - signal_ts_ms) if signal_ts_ms > 0 else 0

        # ENV caps (absolute upper bounds, independent of horizon)
        atr_age_cap_ms = _safe_int(os.getenv("ATR_HORIZON_DQ_ATR_AGE_CAP_MS", "300000"), 300000)
        book_age_cap_ms = _safe_int(os.getenv("ATR_HORIZON_DQ_BOOK_AGE_CAP_MS", "60000"), 60000)
        signal_age_cap_ms = _safe_int(os.getenv("ATR_HORIZON_DQ_SIGNAL_AGE_CAP_MS", "300000"), 300000)

        # Horizon-proportional budgets
        if hold_target_ms > 0:
            atr_age_budget_ms = min(max(1000, int(hold_target_ms * 0.25)), atr_age_cap_ms)
            book_age_budget_ms = min(max(500, int(hold_target_ms * 0.05)), book_age_cap_ms)
        else:
            atr_age_budget_ms = atr_age_cap_ms
            book_age_budget_ms = book_age_cap_ms

        if max_signal_age_ms > 0:
            signal_age_budget_ms = min(max_signal_age_ms, signal_age_cap_ms)
        elif alpha_half_life_ms > 0:
            signal_age_budget_ms = min(alpha_half_life_ms, signal_age_cap_ms)
        else:
            signal_age_budget_ms = signal_age_cap_ms

        # Evaluation (ordered by severity)
        allow_shadow = True
        reason = "DQ_HZ_OK"
        if atr_selected_value <= 0.0:
            allow_shadow = False
            reason = "DQ_ATR_UNAVAILABLE_SELECTED"
        elif atr_selected_age_ms > atr_age_budget_ms:
            allow_shadow = False
            reason = "DQ_ATR_STALE_FOR_HORIZON"
        elif book_ts_ms > 0 and book_age_ms > book_age_budget_ms:
            allow_shadow = False
            reason = "DQ_BOOK_STALE_FOR_HORIZON"
        elif signal_ts_ms > 0 and signal_age_ms > signal_age_budget_ms:
            allow_shadow = False
            reason = "DQ_SIGNAL_TOO_OLD_FOR_HORIZON"

        return asdict(HorizonDQShadow(
            allow_shadow=allow_shadow,
            shadow_reason_code=reason,
            atr_selected_value=atr_selected_value,
            atr_selected_tf_ms=atr_selected_tf_ms,
            atr_selected_age_ms=atr_selected_age_ms,
            atr_age_budget_ms=atr_age_budget_ms,
            book_age_budget_ms=book_age_budget_ms,
            signal_age_budget_ms=signal_age_budget_ms,
            selector_reason_code=selector_reason_code,
            reason_details={
                "hold_target_ms": hold_target_ms,
                "alpha_half_life_ms": alpha_half_life_ms,
                "max_signal_age_ms": max_signal_age_ms,
                "book_age_ms": book_age_ms,
                "signal_age_ms": signal_age_ms,
            }
        ))
    except Exception:
        # Absolute fail-open: never raise into caller's hot path
        return {
            "allow_shadow": True,
            "shadow_reason_code": "DQ_HZ_INTERNAL_ERROR",
            "atr_selected_value": 0.0,
            "atr_selected_tf_ms": 0,
            "atr_selected_age_ms": 0,
            "atr_age_budget_ms": 0,
            "book_age_budget_ms": 0,
            "signal_age_budget_ms": 0,
            "selector_reason_code": "",
            "reason_details": {},
        }
