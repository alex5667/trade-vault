from __future__ import annotations

import hashlib
from typing import Any

from core.meta_features_v1 import META_FEAT_V1_COLS
from core.meta_features_v2 import META_FEAT_V2_NEW_COLS, build_meta_features_v2

META_FEAT_V3_NAME = "meta_feat_v3"
META_FEAT_V3_VERSION = 3

# New columns in V3 (Burst / Hawkes / Churn)
META_FEAT_V3_NEW_COLS = [
    # Burst / Hawkes Logic
    "burst_pen",
    "burst_ctr",    # Cancel-to-Trade Ratio
    "burst_exc",    # Hawkes Excess
    "burst_churn",  # Book Churn Score
    "burst_z",      # Book Rate Z
    "burst_tr_ema", # Trade Rate EMA
    "burst_cr_ema", # Cancel Rate EMA
    "burst_ha_lam", # Hawkes Combined Lam
    # Extra robustness
    "burst_veto_flag", # 0 or 1
]

# Full canonical inventory for V3
META_FEAT_V3_COLS = META_FEAT_V1_COLS + META_FEAT_V2_NEW_COLS + META_FEAT_V3_NEW_COLS

META_FEAT_V3_HASH = hashlib.sha256(
    (",".join(META_FEAT_V3_COLS)).encode("utf-8")
).hexdigest()[:16]

# Default transforms (extending V2)
META_FEAT_V3_TRANSFORMS: dict[str, dict[str, Any]] = {
    # Rates/Excess are strictly positive, log1p is usually good
    "burst_ctr": {"name": "log1p"},
    "burst_exc": {"name": "log1p"},
    "burst_churn": {"name": "log1p"},
    "burst_tr_ema": {"name": "log1p"},
    "burst_cr_ema": {"name": "log1p"},
    "burst_ha_lam": {"name": "log1p"},
    # Pen/Veto are bounded/binary, no transform needed
}

def build_meta_features_v3(
    evidence: dict[str, Any],
    indicators: dict[str, Any],
    runtime_snap: Any | None = None, # book_state.snap
    runtime_prev_snap: Any | None = None, # book_state.prev_snap
    indicators_with_v4: dict[str, Any] | None = None,
    legs: dict[str, Any] | None = None,
    have: int = 0,
    need: int = 0,
    ok_soft: int = 0,
    rule_score: float = 0.0,
    exec_risk_norm: float = 0.0,
    exec_risk_bps: float = 0.0,
    ml_scenario: str = "",
) -> tuple[dict[str, float], list[str]]:
    """
    Builds meta_feat_v3 features.
    Delegates to v2 for base features, then adds burst/hawkes v3 features.
    """

    # 1. Base V2 (which calls V1)
    feat, missing = build_meta_features_v2(
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

    # 2. Add V3 Burst Features
    # Expectation: 'evidence' contains keys from eval_burst_gate() snapshot
    # If not present (e.g. old logs), we set 0.0 and mark missing

    for k in META_FEAT_V3_NEW_COLS:
        # Check evidence first (where we put them in engine)
        if k == "burst_veto_flag":
             # Special mapping: burst_veto -> burst_veto_flag
             val = evidence.get("burst_veto")
             if val is not None:
                 feat[k] = float(val)
             else:
                 feat[k] = 0.0
                 missing.append(k)
        elif k in evidence:
            try:
                feat[k] = float(evidence[k])
            except (ValueError, TypeError):
                feat[k] = 0.0
                missing.append(k)
        else:
            # Fallback to indicators if not in evidence (some fields might be redundant)
            if k in indicators:
                 try:
                    feat[k] = float(indicators[k])
                 except (ValueError, TypeError):
                    feat[k] = 0.0
                    missing.append(k)
            else:
                feat[k] = 0.0
                missing.append(k)

    return feat, missing
