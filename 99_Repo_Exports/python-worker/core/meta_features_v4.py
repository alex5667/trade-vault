from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Tuple, Optional

from core.meta_features_v3 import META_FEAT_V3_COLS, build_meta_features_v3
from core.book_microstructure_v4 import compute_microstructure_v4

META_FEAT_V4_NAME = "meta_feat_v4"
META_FEAT_V4_VERSION = 4
META_FEAT_V4_VERSION_STR = "v4"

# New columns in V4
META_FEAT_V4_NEW_COLS = [
    "mp_mid_bps",
    "mp_shift_bps",
    "depth_bid_5",
    "depth_ask_5",
    "book_slope_bid",
    "book_slope_ask",
    "book_convex_bid",
    "book_convex_ask",
    "obi_dw"
]

# Full canonical inventory for V4
META_FEAT_V4_COLS = META_FEAT_V3_COLS + META_FEAT_V4_NEW_COLS

META_FEAT_V4_HASH = hashlib.sha256(
    (",".join(META_FEAT_V4_COLS)).encode("utf-8")
).hexdigest()[:16]

# Default transforms
META_FEAT_V4_TRANSFORMS: Dict[str, Dict[str, Any]] = {
    # Depths are strictly positive, log1p
    "depth_bid_5": {"name": "log1p"},
    "depth_ask_5": {"name": "log1p"},
    # Bps / Slopes / Convexity can be negative or positive, maybe clip or robust scaler?
    # Standard scaler (z-score) is usually applied later.
    # No Log1p for these signed values.
}

def build_meta_features_v4(
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
    Builds meta_feat_v4 features.
    Delegates to v3 for base features, then adds microprice/slope v4 features.
    """
    
    # 1. Base V3 (which calls V2->V1)
    feat, missing = build_meta_features_v3(
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
    
    # 2. Add V4 Microstructure Features
    # We compute these on-the-fly from snaps if available
    
    micro_v4 = {}
    if runtime_snap is not None:
         # Compute V4 features
         micro_v4 = compute_microstructure_v4(runtime_snap, runtime_prev_snap, levels=5)
    
    # 3. Merge into feat
    for k in META_FEAT_V4_NEW_COLS:
        if k in micro_v4:
             feat[k] = float(micro_v4[k])
        
        # Fallback 1: Direct availability in evidence
        elif k in evidence:
             try:
                 feat[k] = float(evidence[k])
             except (ValueError, TypeError):
                 feat[k] = 0.0
                 missing.append(k)

        # Fallback 2: Indicators argument (explicit)
        elif k in indicators:
             try:
                 feat[k] = float(indicators[k])
             except (ValueError, TypeError):
                 feat[k] = 0.0
                 missing.append(k)

        # Fallback 3: Nested in evidence['indicators'] (nightly dataset common case)
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
