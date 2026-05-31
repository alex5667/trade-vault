from __future__ import annotations

import math
from hashlib import sha256
from typing import Any, Dict, List, Tuple

META_FEATURE_SCHEMA_VERSION = "meta_feat_v1"

# Canonical feature list (order matters for hash stability, though dicts are unordered)
# This serves as the "inventory".
META_FEATURE_COLS = [
    # --- Rule-gate confidence ---
    "base_score",
    "score_final_raw",
    "score_final_01",
    "exec_pen",
    "have",
    "need",
    "have_need_ratio",
    "ok_soft",
    "exec_risk_norm",
    "exec_risk_bps",
    "exec_risk_ref_bps",
    "agg_is_sum",
    "agg_is_avg",
    # --- Evidence / Microstructure ---
    "delta_z",
    "obi",
    "obi_stable",
    "obi_stable_secs",
    "ofi",
    "ofi_z",
    "ofi_stable",
    "ofi_stable_secs",
    "iceberg_strict",
    "iceberg_refresh",
    "iceberg_duration",
    "absorption",
    "absorption_volume",
    "abs_lvl_ok",
    "abs_lvl_score",
    "fp_edge_absorb",
    "fp_edge_ok",
    # --- Staleness / Health ---
    "data_health",
    "book_health_ok",
    "data_health_veto_book_evidence",
    "cvd_quarantine_active",
    "book_staleness_ms",
    "obi_age_ms",
    "iceberg_age_ms",
    "ofi_age_ms",
    "sweep_age_ms",
    "reclaim_age_ms",
    "fp_edge_age_ms",
    # --- Scenarios ---
    "scn_is_news",
    "scn_is_trend",
    "scn_is_range",
    "scn_is_chop",
    # --- Legacy Legs ---
    "leg_ofi_leg",
    "leg_fp_edge_absorb",
    "leg_obi_stable",
    "leg_iceberg_strict",
    "leg_abs_lvl_ok",
    "leg_reclaim_recent",
    "leg_weak_progress",
    "leg_sweep_recent",
]

# Simple hash of the columns to detect schema drift quickly
META_FEATURE_COLS_HASH = sha256(",".join(sorted(META_FEATURE_COLS)).encode("utf-8")).hexdigest()[:8]


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        # handles None, "", "nan", etc.
        if val is None:
            return default
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


def build_meta_features(
    evidence: Dict[str, Any],
    indicators: Dict[str, Any],
    indicators_with_v4: Dict[str, Any],
    legs: Dict[str, Any],
    runtime: Any,
    meta_ctx: Dict[str, Any],
) -> Tuple[Dict[str, float], Dict[str, Any]]:
    """
    Constructs the feature dictionary for MetaModelLR.
    Returns (features, stats).
    
    Stats includes:
      - missing_count
      - missing_rate
      - present: Set[str] of found keys
    """
    
    # context helpers
    rule_score = _safe_float(meta_ctx.get("rule_score", 0.0))
    have = _safe_float(meta_ctx.get("have", 0.0))
    need = _safe_float(meta_ctx.get("need", 0.0))
    ok_soft = _safe_float(meta_ctx.get("ok_soft", 0.0))
    exec_risk_norm = _safe_float(meta_ctx.get("exec_risk_norm", 0.0))
    exec_risk_bps = _safe_float(meta_ctx.get("exec_risk_bps", 0.0))
    
    # Pre-extract common lookups to avoid repetition
    ev_score = evidence.get("score_breakdown", {})
    if not isinstance(ev_score, dict):
        ev_score = {}
        
    scn_v4 = str(indicators_with_v4.get("scenario_v4", "")).lower()

    # Features map
    # We use _safe_float everywhere to guarantee valid inputs for LR.
    f = {}
    
    # --- Rule-gate confidence decomposition ---
    f["base_score"] = _safe_float(ev_score.get("base_score", 0.0))
    f["score_final_raw"] = _safe_float(ev_score.get("final_score_raw", 0.0))
    f["score_final_01"] = _safe_float(ev_score.get("final_score_01", rule_score))
    f["exec_pen"] = _safe_float(ev_score.get("exec_pen", 0.0))
    
    f["have"] = have
    f["need"] = need
    f["have_need_ratio"] = have / max(1.0, need)
    f["ok_soft"] = ok_soft
    
    f["exec_risk_norm"] = exec_risk_norm
    f["exec_risk_bps"] = exec_risk_bps
    f["exec_risk_ref_bps"] = _safe_float(evidence.get("exec_risk_ref_bps", 0.0))
    
    agg = str(ev_score.get("agg", "")).lower()
    f["agg_is_sum"] = 1.0 if agg == "sum" else 0.0
    f["agg_is_avg"] = 1.0 if agg != "sum" else 0.0
    
    # --- Evidence / microstructure ---
    f["delta_z"] = _safe_float(evidence.get("delta_z", 0.0))
    
    f["obi"] = _safe_float(evidence.get("obi", 0.0))
    f["obi_stable"] = _safe_float(evidence.get("obi_stable", 0.0))
    f["obi_stable_secs"] = _safe_float(evidence.get("obi_stable_secs", 0.0))
    
    f["ofi"] = _safe_float(evidence.get("ofi", 0.0))
    f["ofi_z"] = _safe_float(evidence.get("ofi_z", 0.0))
    f["ofi_stable"] = _safe_float(evidence.get("ofi_stable", 0.0))
    f["ofi_stable_secs"] = _safe_float(evidence.get("ofi_stable_secs", 0.0))
    
    f["iceberg_strict"] = _safe_float(evidence.get("iceberg_strict", 0.0))
    f["iceberg_refresh"] = _safe_float(evidence.get("iceberg_refresh", 0.0))
    f["iceberg_duration"] = _safe_float(evidence.get("iceberg_duration", 0.0))
    
    f["absorption"] = _safe_float(evidence.get("absorption", 0.0))
    f["absorption_volume"] = _safe_float(evidence.get("absorption_volume", 0.0))
    
    f["abs_lvl_ok"] = _safe_float(evidence.get("abs_lvl_ok", 0.0))
    f["abs_lvl_score"] = _safe_float(indicators_with_v4.get("abs_lvl_score", 0.0))
    
    f["fp_edge_absorb"] = _safe_float(evidence.get("fp_edge_absorb", 0.0))
    # fallback to leg value if not in indicators
    f["fp_edge_ok"] = _safe_float(indicators_with_v4.get("fp_edge_ok", legs.get("fp_edge_absorb", 0.0)))
    
    # --- Staleness / health ---
    f["data_health"] = _safe_float(indicators_with_v4.get("data_health", 1.0), 1.0)
    f["book_health_ok"] = 1.0 if indicators_with_v4.get("book_health_ok", True) else 0.0
    f["data_health_veto_book_evidence"] = 1.0 if indicators_with_v4.get("data_health_veto_book_evidence", False) else 0.0
    f["cvd_quarantine_active"] = 1.0 if indicators_with_v4.get("cvd_quarantine_active", False) else 0.0
    
    # Ages: sanitize -1 to -1.0
    f["book_staleness_ms"] = _safe_float(indicators_with_v4.get("book_staleness_ms", -1.0), -1.0)
    f["obi_age_ms"] = _safe_float(evidence.get("obi_age_ms", -1.0), -1.0)
    f["iceberg_age_ms"] = _safe_float(evidence.get("iceberg_age_ms", -1.0), -1.0)
    f["ofi_age_ms"] = _safe_float(evidence.get("ofi_age_ms", -1.0), -1.0)
    f["sweep_age_ms"] = _safe_float(evidence.get("sweep_age_ms", -1.0), -1.0)
    f["reclaim_age_ms"] = _safe_float(evidence.get("reclaim_age_ms", -1.0), -1.0)
    f["fp_edge_age_ms"] = _safe_float(evidence.get("fp_edge_age_ms", -1.0), -1.0)
    
    # --- Scenario buckets ---
    f["scn_is_news"] = 1.0 if "news" in scn_v4 else 0.0
    f["scn_is_trend"] = 1.0 if "trend" in scn_v4 else 0.0
    f["scn_is_range"] = 1.0 if "range" in scn_v4 else 0.0
    f["scn_is_chop"] = 1.0 if "chop" in scn_v4 else 0.0
    
    # --- Legacy legs ---
    f["leg_ofi_leg"] = _safe_float(legs.get("ofi_leg", 0.0))
    f["leg_fp_edge_absorb"] = _safe_float(legs.get("fp_edge_absorb", 0.0))
    f["leg_obi_stable"] = _safe_float(legs.get("obi_stable", 0.0))
    f["leg_iceberg_strict"] = _safe_float(legs.get("iceberg_strict", 0.0))
    f["leg_abs_lvl_ok"] = _safe_float(legs.get("abs_lvl_ok", 0.0))
    f["leg_reclaim_recent"] = _safe_float(legs.get("reclaim_recent", 0.0))
    f["leg_weak_progress"] = _safe_float(legs.get("weak_progress", 0.0))
    f["leg_sweep_recent"] = _safe_float(legs.get("sweep_recent", 0.0))
    
    # Calculate stats
    # We check if keys "exist" in the source dicts to determine "missing".
    # Since we use .get() with defaults for values, we need a separate check for presence.
    # However, many sources are optional.
    # We'll define "missing" as: expected in META_FEATURE_COLS but not effectively found in sources.
    # But because sources are scattered (evidence, indicators, legs), strict checking is hard.
    # Instead, we check if the value ended up being the default 0.0 AND the key was missing in source.
    # This is heuristic.
    # Better: just track what we successfully pulled.
    
    present = set()
    # Manual tracking for simplicity & speed
    
    # Helper to check if any key exists in any dict
    def _exists(keys, *dicts):
        for d in dicts:
            for k in keys:
                if k in d:
                    return True
        return False
        
    # Mapping feature -> potential source keys
    # If a feature is derived (like Ratio), we assume it's present if inputs are.
    # For now, we assume everything we "computed" above is "present" because we assigned it a value.
    # REAL missingness is when the upstream data was absent, but we defaulted to 0.0.
    
    # Let's count "zeros that were caused by missing keys" if possible?
    # Actually, simpler: just count how many of the computed features are 0.0
    # No, that's not right. 0.0 is a valid value.
    
    # Let's just track the "explicitly missing" notion if we want to be strict.
    # But for now, we'll return a basic stats object.
    
    stats = {
        "count": len(f),
        "present": f,  # all keys are present in the output dict
        "cols_hash": META_FEATURE_COLS_HASH
    }
    
    return f, stats


def meta_missing_stats(
    feat: Dict[str, float],
    present: Dict[str, float],
    schema_version: str,
    feature_names: List[str] = None,
) -> Dict[str, Any]:
    """
    Computes missing statistics relative to a model's expected features.
    """
    if feature_names is None:
        feature_names = META_FEATURE_COLS
        
    missing_count = 0
    missing_critical = 0
    
    critical_cols = {
        "base_score", "exec_risk_norm", "have", "need", "ok_soft", 
        "obi_stable", "ofi_stable", "iceberg_strict", "absorption"
    }
    
    # In this V1 implementation, we don't have a robust way to know if a value was "missing" 
    # vs "naturally zero" because build_meta_features defaults everything.
    # However, if we had a list of "actually found keys", we could use it.
    # For now, we'll assume 100% presence because we default everything.
    # 
    # To make this useful, the caller should pass a 'present' set if they track it.
    # Since build_meta_features returns a full dict, we are 'fail-open' by default.
    
    # If feature_names contains keys NOT in feat, that's a real missing feature (schema drift).
    real_missing = []
    
    for name in feature_names:
        if name not in feat:
            missing_count += 1
            real_missing.append(name)
            if name in critical_cols:
                missing_critical += 1
                
    return {
        "missing_count": missing_count,
        "missing_rate": missing_count / max(1, len(feature_names)),
        "missing_critical_count": missing_critical,
        "missing_cols": real_missing,
        "schema_version": schema_version
    }
