from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Tuple, Optional

from core.meta_features_v4 import META_FEAT_V4_COLS, build_meta_features_v4

META_FEAT_V5_NAME = "meta_feat_v5"
META_FEAT_V5_VERSION = 5
META_FEAT_V5_VERSION_STR = "v5"

# New columns in V5 (DQ / Time Determinism)
META_FEAT_V5_NEW_COLS = [
    "tick_time_age_ms",
    "tick_event_age_abs_ema_ms",  # Canonical name (was tick_time_age_abs_ema_ms)
    "tick_event_stream_skew_abs_ema_ms",
    "tick_ts_source_now_ema",
    "tick_ts_source_stream_id_ema",
    "data_health",
    "book_health_ok",
    "tick_unknown_side_ema",
]

# Full canonical inventory for V5
META_FEAT_V5_COLS = META_FEAT_V4_COLS + META_FEAT_V5_NEW_COLS

META_FEAT_V5_HASH = hashlib.sha256(
    (",".join(META_FEAT_V5_COLS)).encode("utf-8")
).hexdigest()[:16]

# Default transforms
META_FEAT_V5_TRANSFORMS: Dict[str, Dict[str, Any]] = {
    # Most DQ metrics are non-negative.
    # We might want log1p for large time deltas, but for now linear is fine or we can add later.
    # book_health_ok is binary (0/1).
    # tick_time_age_ms can be negative? usually positive lag.
}

def build_meta_features_v5(
    evidence: Dict[str, Any],
    indicators: Dict[str, Any],
    runtime_snap: Optional[Any] = None, # book_state.snap
    runtime_prev_snap: Optional[Any] = None, # book_state.prev_snap
    indicators_with_v4: Optional[Dict[str, Any]] = None,
    legs: Optional[Dict[str, Any]] = None,
    have: int = 0,
    need: int = 0,
    ok_soft: int = 0,
    rule_score: float = 0.0,
    exec_risk_norm: float = 0.0,
    exec_risk_bps: float = 0.0,
    ml_scenario: str = "",
) -> Tuple[Dict[str, float], List[str]]:
    """
    Builds meta_feat_v5 features.
    Delegates to v4 for base features, then adds DQ/Time features.
    """
    
    # 1. Base V4
    feat, missing = build_meta_features_v4(
        evidence=evidence,
        indicators=indicators,
        runtime_snap=runtime_snap,
        runtime_prev_snap=runtime_prev_snap,
        indicators_with_v4=indicators_with_v4,
        legs=legs,
        have=have,
        need=need,
        ok_soft=ok_soft,
        rule_score=rule_score,
        exec_risk_norm=exec_risk_norm,
        exec_risk_bps=exec_risk_bps,
        ml_scenario=ml_scenario,
    )
    
    # 2. Add V5 DQ Features
    # Source: evidence (primary), indicators, or evidence['indicators']
    
    for k in META_FEAT_V5_NEW_COLS:
        # Priority 1: Direct availability in evidence
        if k in evidence:
             try:
                 feat[k] = float(evidence[k])
             except (ValueError, TypeError):
                 feat[k] = 0.0
                 missing.append(k)
        
        # Priority 1b: Alias for canonical tick_event_age_abs_ema_ms
        elif k == "tick_event_age_abs_ema_ms" and "tick_time_age_abs_ema_ms" in evidence:
             try:
                 feat[k] = float(evidence["tick_time_age_abs_ema_ms"])
             except (ValueError, TypeError):
                 feat[k] = 0.0
                 missing.append(k)

        # Priority 2: Indicators argument
        elif k in indicators:
             try:
                 feat[k] = float(indicators[k])
             except (ValueError, TypeError):
                 feat[k] = 0.0
                 missing.append(k)

        # Priority 3: Nested in evidence['indicators']
        elif "indicators" in evidence and isinstance(evidence["indicators"], dict) and k in evidence["indicators"]:
             try:
                 feat[k] = float(evidence["indicators"][k])
             except (ValueError, TypeError):
                 feat[k] = 0.0
                 missing.append(k)

        else:
             # Missing
             feat[k] = 0.0
             missing.append(k)
             
    return feat, missing
