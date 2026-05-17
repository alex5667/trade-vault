#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import math
import os
import sys
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

# P9: Future behavior opt-in
pd.set_option('future.no_silent_downcasting', True)

from core.feature_engineering import apply_transform

# Builder branching below still discriminates v2..v6 by *function identity*
# (those builders take different kwargs). The NEW_COLS lists and v1 default
# name are also used downstream for column-level missing-mask reporting and
# argparse defaults. Keep these names imported explicitly; everything else
# (cols/hash/transforms/builders for v1..v10) comes from the central registry.
from core.meta_features_v1 import META_FEAT_V1_NAME
from core.meta_features_v2 import META_FEAT_V2_NEW_COLS, build_meta_features_v2
from core.meta_features_v3 import META_FEAT_V3_NEW_COLS, build_meta_features_v3
from core.meta_features_v4 import META_FEAT_V4_NEW_COLS, build_meta_features_v4
from core.meta_features_v5 import build_meta_features_v5
from core.meta_features_v6 import build_meta_features_v6
from core.meta_model_lr import MetaModelLR

# Train==Serve: derive schema metadata from the central registry so v7..v10
# (and any future schemas) are trainable without re-listing them here.
from core.meta_schema_registry import (
    META_SCHEMA_BUILDERS,
    META_SCHEMA_REGISTRY,
    META_SCHEMA_TRANSFORMS,
)

SCHEMAS = {
    name: {
        "version": ver,
        "cols": cols,
        "hash": h,
        "transforms": META_SCHEMA_TRANSFORMS[name],
        "builder": META_SCHEMA_BUILDERS[name],
    }
    for name, (ver, cols, h) in META_SCHEMA_REGISTRY.items()
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("train_meta")

def robust_center_scale(x: np.ndarray) -> tuple[float, float]:
    """Compute robust center (median) and scale (MAD)."""
    # median / MAD (scaled)
    med = float(np.nanmedian(x))
    mad = float(np.nanmedian(np.abs(x - med)))
    scale = mad * 1.4826
    if (not math.isfinite(scale)) or scale < 1e-9:
        scale = 1.0
    if not math.isfinite(med):
        med = 0.0
    return med, scale

def build_row_features(
    row: dict[str, Any],
    builder_func: Any
) -> dict[str, float]:
    """
    Constructs features from a dictionary row (parquet record).
    Note: 'row' is acting as evidence, indicators, etc.
    """
    have = int(row.get("have", 0) or 0)
    need = int(row.get("need", 0) or 0)
    ok_soft = int(row.get("ok_soft", 0) or 0)
    rule_score = float(row.get("score_final_01", row.get("rule_score", 0.0)) or 0.0)
    exec_risk_norm = float(row.get("exec_risk_norm", 0.0) or 0.0)
    exec_risk_bps = float(row.get("exec_risk_bps", 0.0) or 0.0)
    ml_scenario = (row.get("scenario_v4", row.get("ml_scenario", "")) or "")

    feat: dict[str, float] = {}

    # We simply invoke the builder. The builder for V2 signatures handles the specific args if we pass correctly.
    # But here we invoke strictly by what the function expects or generic args.
    # Since V1 and V2 have different signatures in my implementation, I need to branch.

    if builder_func == build_meta_features_v2:
        feat, _ = builder_func(
            evidence=row,
            indicators=row,
            runtime_snap=None, # Not available in simple parquet
            runtime_prev_snap=None,
            indicators_with_v4=row,
            legs=row,
            have=have,
            need=need,
            ok_soft=ok_soft,
            rule_score=rule_score,
            exec_risk_norm=exec_risk_norm,
            exec_risk_bps=exec_risk_bps,
            ml_scenario=ml_scenario,
        )
    elif builder_func == build_meta_features_v3:
        feat, _ = builder_func(
            evidence=row,
            indicators=row,
            runtime_snap=None,
            runtime_prev_snap=None,
            indicators_with_v4=row,
            legs=row,
            have=have,
            need=need,
            ok_soft=ok_soft,
            rule_score=rule_score,
            exec_risk_norm=exec_risk_norm,
            exec_risk_bps=exec_risk_bps,
            ml_scenario=ml_scenario,
        )
        feat, _ = builder_func(
            evidence=row,
            indicators=row,
            runtime_snap=None, # Not available in simple parquet
            runtime_prev_snap=None,
            indicators_with_v4=row,
            legs=row,
            have=have,
            need=need,
            ok_soft=ok_soft,
            rule_score=rule_score,
            exec_risk_norm=exec_risk_norm,
            exec_risk_bps=exec_risk_bps,
            ml_scenario=ml_scenario,
        )
    elif builder_func == build_meta_features_v4 or builder_func == build_meta_features_v5:
        feat, _ = builder_func(
            evidence=row,
            indicators=row,
            runtime_snap=None, # Not available in simple parquet
            runtime_prev_snap=None,
            indicators_with_v4=row,
            legs=row,
            have=have,
            need=need,
            ok_soft=ok_soft,
            rule_score=rule_score,
            exec_risk_norm=exec_risk_norm,
            exec_risk_bps=exec_risk_bps,
            ml_scenario=ml_scenario,
        )
    elif builder_func == build_meta_features_v6:
        feat, _ = builder_func(
            evidence=row,
            indicators=row,
            runtime_snap=None,
            runtime_prev_snap=None,
            indicators_with_v4=row,
            legs=row,
            have=have,
            need=need,
            ok_soft=ok_soft,
            rule_score=rule_score,
            exec_risk_norm=exec_risk_norm,
            exec_risk_bps=exec_risk_bps,
            ml_scenario=ml_scenario,
        )
    elif builder_func.__name__ in (
        "build_meta_features_v7",
        "build_meta_features_v8",
        "build_meta_features_v9",
        "build_meta_features_v10",
    ):
        # v7..v10 are pass-through builders: (evidence, indicators, **kwargs).
        # They consume `indicators_with_v4`/`runtime_snap` etc. when available
        # but tolerate absence — for parquet rows we just pass the row twice.
        feat, _ = builder_func(
            evidence=row,
            indicators=row,
            runtime_snap=None,
            runtime_prev_snap=None,
            indicators_with_v4=row,
            legs=row,
            have=have,
            need=need,
            ok_soft=ok_soft,
            rule_score=rule_score,
            exec_risk_norm=exec_risk_norm,
            exec_risk_bps=exec_risk_bps,
            ml_scenario=ml_scenario,
        )
    else:
        # V1
        feat, _ = builder_func(
            evidence=row,
            indicators=row,
            indicators_with_v4=row,
            legs=row,
            have=have,
            need=need,
            ok_soft=ok_soft,
            rule_score=rule_score,
            exec_risk_norm=exec_risk_norm,
            exec_risk_bps=exec_risk_bps,
            ml_scenario=ml_scenario,
        )

    return feat

def main() -> int:
    ap = argparse.ArgumentParser(description="Train MetaModelLR with Schema Versioning")
    # P9b: Tolerant args (hyphens or underscores)
    ap.add_argument("--in-parquet", "--in_parquet", dest="in_parquet", required=True, help="Input parquet file")
    ap.add_argument("--out-json", "--out_json", dest="out_json", required=True, help="Output JSON model file")
    ap.add_argument("--schema", default=META_FEAT_V1_NAME, choices=SCHEMAS.keys(), help="Feature schema to use")
    ap.add_argument("--label-col", "--label_col", dest="label_col", default="y", help="Binary label column name")
    ap.add_argument("--C", type=float, default=1.0, help="Inverse of regularization strength")
    ap.add_argument("--max-iter", "--max_iter", dest="max_iter", type=int, default=2000)
    ap.add_argument("--self-check", "--self_check", dest="self_check", type=int, default=1, help="1=enable train==serve check")
    ap.add_argument("--self-check-n", type=int, default=2000)
    args = ap.parse_args()

    # Load configuration based on schema
    schema_cfg = SCHEMAS[args.schema]
    cols = schema_cfg["cols"]
    transforms = schema_cfg["transforms"]
    builder = schema_cfg["builder"]

    print(f"Training with schema: {args.schema} (v{schema_cfg['version']})")
    print(f"Features: {len(cols)}")

    # Load Data
    try:
        df = pd.read_parquet(args.in_parquet)
    except Exception as e:
        print(f"Error reading parquet {args.in_parquet}: {e}")
        return 1

    if args.label_col not in df.columns:
        print(f"Error: label_col '{args.label_col}' not found in dataframe")
        return 1

    if df.empty:
         print("Error: Input dataframe is empty")
         return 1

    y = df[args.label_col].astype(int).to_numpy()
    records = df.to_dict(orient="records")

    n = len(records)
    m = len(cols)
    print(f"Loaded {n} records.")

    # Build Features matrix X
    # Vectorized Feature Construction
    print("Building features (Vectorized)...")

    # 1. Base dataframe operations
    # Helper to safely get column or zeros
    def get_col(name, default=0.0):
        if name in df:
            return df[name].fillna(default)
        return pd.Series(np.full(n, default), index=df.index)

    # 2. Handle nested columns
    # Extract ONLY required columns from 'indicators' if present
    if "indicators" in df.columns:
        logger.info("Extracting required features from 'indicators' column...")
        required_from_ind = [c for c in cols if c not in df.columns]
        if "score" in cols and "score" not in df.columns: # mapping for rule_score
             required_from_ind.append("score")

        if required_from_ind:
            valid_mask = df["indicators"].notna()
            if valid_mask.any():
                try:
                    for c in set(required_from_ind):
                        # Use .apply(get) for selective extraction
                        df[c] = df.loc[valid_mask, "indicators"].apply(lambda d: d.get(c) if isinstance(d, dict) else None)
                    # No need to drop original indicators yet if other parts use it,
                    # but we've avoided full expansion.
                except Exception as e:
                    logger.warning(f"Failed to extract features from indicators: {e}")

    # Handle 'score_breakdown' if present (nested dict)
    sb_cols = {}
    if "score_breakdown" in df:
        # Fast expansion of list-of-dicts
        # Filter None/NaN first to avoid crashes
        valid_mask = df["score_breakdown"].notna()
        if valid_mask.any():
            try:
                # This can be memory intensive but fastest way to unpack
                # We only need specific fields
                sb_df = pd.json_normalize(df.loc[valid_mask, "score_breakdown"])
                # align index
                sb_df.index = df.index[valid_mask]

                for k in ["base_score", "final_score_raw", "final_score_01", "exec_pen", "agg"]:
                     if k in sb_df:
                         sb_cols[k] = sb_df[k].reindex(df.index)
            except Exception as e:
                logger.warning(f"Failed to unpack score_breakdown: {e}")

    # Baseline mapping for common inconsistent names
    if "score" in df and "rule_score" not in df:
        df["rule_score"] = df["score"]

    # 3. Construct Feature Columns
    X_dict = {}

    # --- Rule-gate confidence ---
    # From score_breakdown or fallback
    X_dict["base_score"] = sb_cols.get("base_score", get_col("base_score")).fillna(0.0).astype(float)
    X_dict["score_final_raw"] = sb_cols.get("final_score_raw", get_col("score_final_raw")).fillna(0.0).astype(float)

    # rule_score / score_final_01 alignment
    # Serve logic: row.get("score_final_01", row.get("rule_score", 0.0))
    # We must prioritize score_final_01 for the "rule_score" feature column.
    sf01_col = get_col("score_final_01", np.nan)
    rs_col = get_col("rule_score", 0.0)
    X_dict["rule_score"] = sf01_col.fillna(rs_col).fillna(0.0).astype(float)

    # Also provide score_final_01 if schema needs it (though it maps to rule_score usually)
    X_dict["score_final_01"] = X_dict["rule_score"]

    # exec_pen alignment
    # Serve logic (v6) looks in evidence/indicators, NOT score_breakdown (unless nested indicators has it).
    # To match Serve, we should rely on get_col("exec_pen") which mimics fetching from evidence/indicators logic if flattened.
    # If we pull from sb_cols["exec_pen"] and Serve doesn't, we get mismatch.
    # We will prioritize flattened column to ensure alignment, but fallback to sb_cols if needed
    # PROVIDED we know Serve can find it.
    # actually, since mismatch is the error, we must match Serve's limitations.
    X_dict["exec_pen"] = get_col("exec_pen").fillna(0.0).astype(float)

    have = get_col("have").astype(float)
    need = get_col("need").astype(float)
    X_dict["have"] = have
    X_dict["need"] = need

    # have_need_ratio alignment
    # Serve logic (v6): float(have) / float(need) if need > 0 else 0.0
    numerator = have
    denominator = need
    # Vectorized: where(need > 0, have/need, 0.0)
    X_dict["have_need_ratio"] = np.where(denominator > 0, numerator / denominator, 0.0)

    X_dict["ok_soft"] = get_col("ok_soft").astype(float)
    X_dict["exec_risk_norm"] = get_col("exec_risk_norm").astype(float)
    X_dict["exec_risk_bps"] = get_col("exec_risk_bps").astype(float)
    X_dict["exec_risk_ref_bps"] = get_col("exec_risk_ref_bps").astype(float)

    # agg branching
    agg_val = sb_cols.get("agg", get_col("agg", ""))
    agg_str = agg_val.fillna("").astype(str).str.lower()
    X_dict["agg_is_sum"] = (agg_str == "sum").astype(float)
    X_dict["agg_is_avg"] = (agg_str != "sum").astype(float)

    # --- Evidence / Microstructure ---
    simple_cols_v1 = [
        "delta_z", "obi", "obi_stable", "obi_stable_secs",
        "ofi", "ofi_z", "ofi_stable", "ofi_stable_secs",
        "iceberg_strict", "iceberg_refresh", "iceberg_duration",
        "absorption", "absorption_volume", "abs_lvl_ok", "abs_lvl_score",
        "fp_edge_absorb"
    ]
    for c in simple_cols_v1:
        X_dict[c] = get_col(c).astype(float)

    # fp_edge_ok fallback
    # f["fp_edge_ok"] = indicators_with_v4.get("fp_edge_ok", legs.get("fp_edge_absorb", 0.0))
    # both are in df (row)
    fp_ok = get_col("fp_edge_ok", np.nan)
    fp_abs = get_col("fp_edge_absorb", 0.0) # leg name in df is same as evidence name usually?
    # V1: legs.get("fp_edge_absorb")
    # In flat df, likely "fp_edge_absorb" is the column.
    X_dict["fp_edge_ok"] = fp_ok.fillna(fp_abs).astype(float)

    # --- Health ---
    X_dict["data_health"] = get_col("data_health", 1.0).astype(float)
    # booleans
    X_dict["book_health_ok"] = get_col("book_health_ok", 1.0).astype(bool).astype(float)
    X_dict["data_health_veto_book_evidence"] = get_col("data_health_veto_book_evidence", 0.0).astype(bool).astype(float)
    X_dict["cvd_quarantine_active"] = get_col("cvd_quarantine_active", 0.0).astype(bool).astype(float)

    # Ages
    for c in ["book_staleness_ms", "obi_age_ms", "iceberg_age_ms", "ofi_age_ms",
              "sweep_age_ms", "reclaim_age_ms", "fp_edge_age_ms"]:
        X_dict[c] = get_col(c, -1.0).astype(float)

    # --- Scenarios ---
    scenario_col = get_col("scenario_v4", "").astype(str).str.lower()
    # Fallback to ml_scenario if scenario_v4 empty?
    # V1: scn_v4 = (indicators_with_v4.get("scenario_v4", "")).lower()
    # row mapping: ml_scenario = row.get("scenario_v4", row.get("ml_scenario"))
    if "ml_scenario" in df:
        scenario_col = np.where(scenario_col == "", df["ml_scenario"].fillna("").astype(str).str.lower(), scenario_col)

    X_dict["scn_is_news"] = scenario_col.str.contains("news").astype(float)
    X_dict["scn_is_trend"] = scenario_col.str.contains("trend").astype(float)
    X_dict["scn_is_range"] = scenario_col.str.contains("range").astype(float)
    X_dict["scn_is_chop"] = scenario_col.str.contains("chop").astype(float)

    # --- Legacy Legs ---
    # Map: name in feature dict -> name in df
    leg_map = {
        "leg_ofi_leg": "ofi_leg",
        "leg_fp_edge_absorb": "fp_edge_absorb", # duplicate of above? V1 uses legs.get("fp_edge_absorb")
        "leg_obi_stable": "obi_stable", # note: collision with evidence "obi_stable".
        # But 'row' has ONE value for 'obi_stable'.
        # In V1 build_meta_features: f["obi_stable"] = evidence.get("obi_stable"). f["leg_obi_stable"] = legs.get("obi_stable").
        # If 'row' is passed as EVERYTHING, then they are satisfied by the SAME column 'obi_stable'.
        "leg_iceberg_strict": "iceberg_strict",
        "leg_abs_lvl_ok": "abs_lvl_ok",
        "leg_reclaim_recent": "reclaim_recent",
        "leg_weak_progress": "weak_progress",
        "leg_sweep_recent": "sweep_recent",
    }
    for feat_name, col_name in leg_map.items():
        X_dict[feat_name] = get_col(col_name).astype(float)

    # --- V2 Helpers ---
    # qimb_lX, ofi_ml_X - direct mapping
    for c in META_FEAT_V2_NEW_COLS:
        X_dict[c] = get_col(c).astype(float)

    # --- V3 Helpers ---
    for c in META_FEAT_V3_NEW_COLS:
        if c == "burst_veto_flag":
            X_dict[c] = get_col("burst_veto", 0.0).astype(float)
        else:
            X_dict[c] = get_col(c).astype(float)

    # --- V4 Helpers ---
    for c in META_FEAT_V4_NEW_COLS:
        X_dict[c] = get_col(c).astype(float)

    # --- Exhaustive Fallback ---
    # Ensure ALL columns required by the schema are in X_dict
    for c in cols:
        if c not in X_dict:
            X_dict[c] = get_col(c).astype(float)

    # Assemble X_raw
    X_raw = np.column_stack([X_dict[c] for c in cols])

    # Fill remaining NaNs (if any slipped through) with 0.0
    X_raw = np.nan_to_num(X_raw, nan=0.0, posinf=0.0, neginf=0.0)

    # Apply Transforms
    X_tf = X_raw.copy()
    for j, name in enumerate(cols):
        spec = transforms.get(name)
        if spec:
            vfunc = np.vectorize(lambda v: apply_transform(float(v), spec))
            X_tf[:, j] = vfunc(X_tf[:, j])

    # Robust Scaling
    centers = np.zeros(m, dtype=float)
    scales = np.ones(m, dtype=float)
    for j in range(m):
        c, s = robust_center_scale(X_tf[:, j])
        centers[j] = c
        scales[j] = s

    # Normalize X
    X = (X_tf - centers) / scales

    # Train Model
    print("Fitting Logistic Regression...")
    lr = LogisticRegression(
        C=float(args.C),
        max_iter=int(args.max_iter),
        class_weight="balanced",
        solver="lbfgs",
    )
    lr.fit(X, y)

    # Export Model
    robust_scaler_params = {
        name: {"center": float(centers[j]), "scale": float(scales[j])}
        for j, name in enumerate(cols)
    }

    # Helper to construct RobustScalerPack is inside MetaModelLR.load
    # But for dump we can pass explicit robust_scaler object or rely on manual update after init.
    # We will use the manual update approach or a simpler dict-based if MetaModelLR supports it?
    # MetaModelLR expects RobustScalerPack in robust_scaler field.
    # Let's import RobustScalerPack to be safe.
    try:
        from core.feature_engineering import RobustScalerPack
        # RobustScalerPack params is Dict[str, Dict[str, float]]
        rs_pack = RobustScalerPack(params=robust_scaler_params)
    except ImportError:
        # Fallback if imports missing in some env (shouldn't happen in worker)
        print("Warning: Could not import RobustScalerPack")
        rs_pack = None

    mm = MetaModelLR(
        features=list(cols),
        intercept=float(lr.intercept_[0]),
        coef=[float(x) for x in lr.coef_[0].tolist()],
        threshold=0.5,
        transforms=dict(transforms),
        robust_scaler=rs_pack,
        schema_name=args.schema,
        schema_version=int(schema_cfg["version"]),
        schema_hash=str(schema_cfg["hash"]),
        feature_cols_hash=str(schema_cfg["hash"]),
    )

    os.makedirs(os.path.dirname(os.path.abspath(args.out_json)), exist_ok=True)
    mm.dump(args.out_json)
    print(f"Model saved to {args.out_json}")

    # Self-Check
    if int(args.self_check) == 1:
        print("Running Train==Serve self-check...")
        mm_loaded = MetaModelLR.load(args.out_json)
        k = min(int(args.self_check_n), n)

        # Reconstruct feat_dicts only for the check subset (slow path, but limited to k)
        val_dicts = []
        for i in range(k):
            val_dicts.append(build_row_features(records[i], builder))

        p_rt = np.array([mm_loaded.predict_proba(val_dicts[i]) for i in range(k)], dtype=float)
        p_sk = lr.predict_proba(X[:k])[:, 1]
        err = float(np.max(np.abs(p_rt - p_sk)))
        if err > 1e-6:
            print(f"ERROR: Train==Serve mismatch: max_abs_err={err:.3e} > 1e-6")
            idx = np.argmax(np.abs(p_rt - p_sk))
            print(f"Mismatch at idx {idx}: RT={p_rt[idx]:.6f}, SK={p_sk[idx]:.6f}")
            return 1
        else:
            print(f"Self-check PASSED: max_abs_err={err:.3e}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
