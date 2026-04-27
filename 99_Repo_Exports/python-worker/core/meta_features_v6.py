import hashlib
from typing import Any, Dict, List, Optional, Tuple

from core.meta_features_v5 import (
    META_FEAT_V5_COLS,
    META_FEAT_V5_HASH,
    META_FEAT_V5_NAME,
    META_FEAT_V5_VERSION,
    build_meta_features_v5,
)

# ---------------------------------------------------------------------
# META-FEATURE SCHEMA V6
#
# Goal (P1): Expand schema using already-computed runtime indicators
# (no new detectors), so Train == Serve and we avoid silent feature drift.
# ---------------------------------------------------------------------

META_FEAT_V6_NAME = "meta_feat_v6"
META_FEAT_V6_VERSION = 6

# New fields (already computed in runtime / strategy / evidence):
# - Execution / scoring
# - Staleness / health
# - Runtime micro stability
# - L3-lite + Hawkes-like online intensities
META_FEAT_V6_NEW_COLS: List[str] = [
    # Execution + score core
    "exec_risk_ref_bps",
    "exec_pen",
    "of_base_score",
    "of_score_final_raw",
    "have_need_ratio",

    # Staleness / health
    "book_staleness_ms",
    "obi_age_ms",
    "ofi_age_ms",
    "iceberg_age_ms",
    "fp_edge_age_ms",
    "sweep_age_ms",
    "reclaim_age_ms",
    "source_consistency_ok",

    # Runtime stability
    "last_spread_z",
    "book_rate_z",
    "book_churn_score",
    "pressure_sps",
    "cooldown_hit_rate_ema",

    # L3-lite EMA rates
    "taker_buy_rate_ema",
    "taker_sell_rate_ema",
    "cancel_bid_rate_ema",
    "cancel_ask_rate_ema",

    # Hawkes-like online intensities
    "hawkes_taker_lam",
    "hawkes_cancel_lam",
    "hawkes_churn_lam",
]

META_FEAT_V6_COLS: List[str] = list(META_FEAT_V5_COLS) + list(META_FEAT_V6_NEW_COLS)
META_FEAT_V6_HASH: str = hashlib.sha1(",".join(META_FEAT_V6_COLS).encode("utf-8")).hexdigest()

# Transform specs are applied by core.feature_engineering.apply_transform.
# NOTE: v3/v4 currently use 'name' key; apply_transform has a backwards-compatible alias.
META_FEAT_V6_TRANSFORMS: Dict[str, Dict[str, Any]] = {
    # Age / staleness: log1p (non-negative, but can be large)
    "book_staleness_ms": {"type": "log1p"},
    "obi_age_ms": {"type": "log1p"},
    "ofi_age_ms": {"type": "log1p"},
    "iceberg_age_ms": {"type": "log1p"},
    "fp_edge_age_ms": {"type": "log1p"},
    "sweep_age_ms": {"type": "log1p"},
    "reclaim_age_ms": {"type": "log1p"},

    # Execution: bps / penalties
    "exec_risk_ref_bps": {"type": "log1p"},
    "exec_pen": {"type": "clip", "lo": 0.0, "hi": 1.5},

    # Scores: mostly bounded-ish, but clip for stability
    "of_base_score": {"type": "clip", "lo": -5.0, "hi": 5.0},
    "of_score_final_raw": {"type": "clip", "lo": -5.0, "hi": 5.0},
    "have_need_ratio": {"type": "clip", "lo": 0.0, "hi": 5.0},

    # Z-like: clip
    "last_spread_z": {"type": "clip", "lo": -8.0, "hi": 8.0},
    "book_rate_z": {"type": "clip", "lo": -8.0, "hi": 8.0},

    # Non-negative rates: log1p
    "pressure_sps": {"type": "log1p"},
    "taker_buy_rate_ema": {"type": "log1p"},
    "taker_sell_rate_ema": {"type": "log1p"},
    "cancel_bid_rate_ema": {"type": "log1p"},
    "cancel_ask_rate_ema": {"type": "log1p"},
    "hawkes_taker_lam": {"type": "log1p"},
    "hawkes_cancel_lam": {"type": "log1p"},
    "hawkes_churn_lam": {"type": "log1p"},
}


def _try_get_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def build_meta_features_v6(
    evidence: Dict[str, Any],
    indicators: Dict[str, Any],
    runtime_snap: Optional[Any] = None,  # book_state.snap
    runtime_prev_snap: Optional[Any] = None,  # book_state.prev_snap
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
    """Build meta_feat_v6 (v5 base + expanded runtime indicators)."""

    feat, missing = build_meta_features_v5(
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

    src = indicators_with_v4 if isinstance(indicators_with_v4, dict) else {}
    nested_ind = evidence.get("indicators") if isinstance(evidence, dict) else None
    if not isinstance(nested_ind, dict):
        nested_ind = {}

    # Derived / canonical fallbacks
    hn = _try_get_float(src.get("have_need_ratio"))
    if hn is None:
        try:
            hn = float(have) / float(need) if need > 0 else 0.0
        except Exception:
            hn = 0.0

    # Fill new columns with priority: evidence -> indicators_with_v4 -> indicators -> evidence['indicators']
    for k in META_FEAT_V6_NEW_COLS:
        v = None

        if k == "have_need_ratio":
            v = hn
        elif k in evidence:
            v = _try_get_float(evidence.get(k))
        elif k in src:
            v = _try_get_float(src.get(k))
        elif k in indicators:
            v = _try_get_float(indicators.get(k))
        elif k in nested_ind:
            v = _try_get_float(nested_ind.get(k))

        if v is None:
            feat[k] = 0.0
            missing.append(k)
        else:
            feat[k] = float(v)

    return feat, missing
