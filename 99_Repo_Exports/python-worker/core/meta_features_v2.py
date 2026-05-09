from __future__ import annotations

import hashlib
from typing import Any

from core.book_microstructure_v2 import compute_ofi_multilevel_topn, compute_queue_imbalance_topn
from core.meta_features_v1 import META_FEAT_V1_COLS, build_meta_features_v1

META_FEAT_V2_NAME = "meta_feat_v2"
META_FEAT_V2_VERSION = 2

# New columns in V2
META_FEAT_V2_NEW_COLS = [
    # Queue Imbalance
    "qimb_l1", "qimb_l2", "qimb_l3", "qimb_l4", "qimb_l5",
    "qimb_wmean",
    # Multi-level OFI
    "ofi_ml",
    "ofi_ml_wsum",
    "ofi_ml_norm",
]

# Full canonical inventory for V2
META_FEAT_V2_COLS = META_FEAT_V1_COLS + META_FEAT_V2_NEW_COLS

META_FEAT_V2_HASH = hashlib.sha256(
    (",".join(META_FEAT_V2_COLS)).encode("utf-8")
).hexdigest()

# Default transforms (extending V1)
META_FEAT_V2_TRANSFORMS: dict[str, dict[str, Any]] = {
    # No specific transforms for [-1, 1] range features like qimb/ofi_norm for now
    # scaler usually handles centering
}

def _build_meta_features_v2_impl(
    evidence: dict[str, Any],
    indicators: dict[str, Any],
    runtime_snap: Any | None = None, # book_state.snap or runtime.last_book
    runtime_prev_snap: Any | None = None, # book_state.prev_snap or runtime.prev_book
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
    Internal implementation of meta_feat_v2 features.
    Delegates to v1 for base features, then adds microstructure v2.
    """

    # 1. Base V1
    feat, missing = build_meta_features_v1(
        evidence=evidence,
        indicators=indicators,
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

    # 2. Add V2 Microstructure

    # QIMB
    # Only if snap is available
    if runtime_snap:
        qimb = compute_queue_imbalance_topn(runtime_snap, levels=5)
        for k in ["qimb_l1", "qimb_l2", "qimb_l3", "qimb_l4", "qimb_l5", "qimb_wmean"]:
            if k in qimb:
                feat[k] = float(qimb[k])
            else:
                missing.append(k)
    else:
        # Snap missing -> Fallback to evidence/indicators for offline evaluation
        for k in ["qimb_l1", "qimb_l2", "qimb_l3", "qimb_l4", "qimb_l5", "qimb_wmean"]:
            if k in evidence:
                try: feat[k] = float(evidence[k])
                except (ValueError, TypeError): feat[k] = 0.0; missing.append(k)
            elif k in indicators:
                try: feat[k] = float(indicators[k])
                except (ValueError, TypeError): feat[k] = 0.0; missing.append(k)
            else:
                feat[k] = 0.0
                missing.append(k)

    # OFI ML
    # Only if snap AND prev_snap available
    if runtime_snap and runtime_prev_snap:
        ofi = compute_ofi_multilevel_topn(runtime_prev_snap, runtime_snap, levels=5)
        for k in ["ofi_ml", "ofi_ml_wsum", "ofi_ml_norm"]:
            if k in ofi:
                feat[k] = float(ofi[k])
            else:
                missing.append(k)
    else:
        # Snap missing -> Fallback to evidence/indicators for offline evaluation
        for k in ["ofi_ml", "ofi_ml_wsum", "ofi_ml_norm"]:
            if k in evidence:
                try: feat[k] = float(evidence[k])
                except (ValueError, TypeError): feat[k] = 0.0; missing.append(k)
            elif k in indicators:
                try: feat[k] = float(indicators[k])
                except (ValueError, TypeError): feat[k] = 0.0; missing.append(k)
            else:
                feat[k] = 0.0
                missing.append(k)

    return feat, missing

def build_meta_features_v2(
    evidence: dict[str, Any],
    indicators: dict[str, Any],
    cfg2: dict[str, Any] | None = None,
    **kwargs: Any
) -> tuple[dict[str, float], list[str]]:
    """
    Universal wrapper for V2 feature builder.
    Supports both:
      - Old signature: (evidence, indicators, runtime_snap=..., ...)
      - New signature: (evidence, indicators, cfg2, ...)
    """

    # Extract args from kwargs or use defaults
    runtime_snap = kwargs.get("runtime_snap")
    runtime_prev_snap = kwargs.get("runtime_prev_snap")
    indicators_with_v4 = kwargs.get("indicators_with_v4")
    legs = kwargs.get("legs")
    have = int(kwargs.get("have", 0))
    need = int(kwargs.get("need", 0))
    ok_soft = int(kwargs.get("ok_soft", 0))
    rule_score = float(kwargs.get("rule_score", 0.0))
    exec_risk_norm = float(kwargs.get("exec_risk_norm", 0.0))
    exec_risk_bps = float(kwargs.get("exec_risk_bps", 0.0))
    ml_scenario = (kwargs.get("ml_scenario", ""))

    return _build_meta_features_v2_impl(
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
