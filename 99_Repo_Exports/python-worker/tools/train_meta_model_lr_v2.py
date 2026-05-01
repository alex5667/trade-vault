#!/usr/bin/env python3
from __future__ import annotations
"""
Train MetaModelLR on datasets produced by build_dataset_from_inputs_outcomes_v2.py.

Input Parquet schema (expected):
- y: int (0/1)
- indicators: dict (full signals:of:inputs payload scrubbed), OR any nested shape that contains rule fields

Outputs:
- JSON artifact compatible with core.meta_model_lr.MetaModelLR.load()
- Optional joblib artifact (dict) for convenience

Usage:
  python python-worker/tools/train_meta_model_lr_v2.py \
      --parquet /var/lib/trade/of_reports/datasets/meta_inputs_outcomes_v2.parquet \
      --out_json /var/lib/trade/of_reports/models/meta_model_lr_v2.json \
      --out_joblib /var/lib/trade/of_reports/models/meta_model_lr_v2.joblib \
      --threshold 0.5
"""

import argparse
import json
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from sklearn.linear_model import LogisticRegression

from core.feature_engineering import apply_transform
from core.meta_features_v1 import (
    META_FEAT_V1_COLS,
    META_FEAT_V1_NAME,
    META_FEAT_V1_VERSION,
    META_FEAT_V1_HASH,
    META_FEAT_V1_TRANSFORMS,
)
from core.meta_features_v2 import (
    META_FEAT_V2_COLS,
    META_FEAT_V2_NAME,
    META_FEAT_V2_VERSION,
    META_FEAT_V2_HASH,
    META_FEAT_V2_TRANSFORMS,
    META_FEAT_V2_NEW_COLS,
)


# Fixed list of columns for Train == Serve consistency (canonical inventory).
# Adjusted by --schema argument.
SCHEMAS = {
    "meta_feat_v1": {
        "cols": META_FEAT_V1_COLS,
        "name": META_FEAT_V1_NAME,
        "version": META_FEAT_V1_VERSION,
        "hash": META_FEAT_V1_HASH,
        "transforms": META_FEAT_V1_TRANSFORMS,
    },
    "meta_feat_v2": {
        "cols": META_FEAT_V2_COLS,
        "name": META_FEAT_V2_NAME,
        "version": META_FEAT_V2_VERSION,
        "hash": META_FEAT_V2_HASH,
        "transforms": META_FEAT_V2_TRANSFORMS,
    },
}

# Simple transform policy (applied before robust scaling) - Overridden by schema transforms if available
TRANSFORMS_LEGACY: Dict[str, List[Dict[str, Any]]] = {
    # Clamp heavy tails
    "delta_z": [{"type": "clip", "lo": -10.0, "hi": 10.0}],
    "ofi_z": [{"type": "clip", "lo": -10.0, "hi": 10.0}],
    "obi_z": [{"type": "clip", "lo": -10.0, "hi": 10.0}],
    "exec_risk_norm": [{"type": "clip", "lo": 0.0, "hi": 10.0}],
    "spread_bps": [{"type": "clip", "lo": 0.0, "hi": 200.0}],
    "expected_slippage_bps": [{"type": "clip", "lo": 0.0, "hi": 200.0}],
    "exec_risk_bps": [{"type": "clip", "lo": 0.0, "hi": 500.0}],
    # Right-skew
    "book_staleness_ms": [{"type": "log1p"}],
}
TRANSFORMS = TRANSFORMS_LEGACY


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return float(default)
        if isinstance(x, (int, float, np.number)):
            v = float(x)
            if not np.isfinite(v):
                return float(default)
            return v
        if isinstance(x, str):
            v = float(x.strip())
            if not np.isfinite(v):
                return float(default)
            return v
    except Exception:
        return float(default)
    return float(default)


def _get_payload(ind: Any) -> Dict[str, Any]:
    # Parquet can store dicts, JSON strings, or nested shapes.
    if isinstance(ind, dict):
        return ind
    if isinstance(ind, str):
        try:
            obj = json.loads(ind)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _get_nested(d: Dict[str, Any], *keys: str) -> Any:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _build_features(payload: Dict[str, Any], direction: str = "", scenario: str = "") -> Dict[str, float]:
    # Support both flat payload and nested {indicators:{...}, evidence:{...}} shapes
    ind = payload.get("indicators") if isinstance(payload.get("indicators"), dict) else payload
    evidence = payload.get("evidence") if isinstance(payload.get("evidence"), dict) else payload.get("evidence", {})  # type: ignore
    if not isinstance(evidence, dict):
        evidence = {}

    legs = payload.get("legs")
    if not isinstance(legs, dict):
        legs = _get_nested(evidence, "legs")
    if not isinstance(legs, dict):
        legs = {}

    sb = ind.get("score_breakdown") if isinstance(ind.get("score_breakdown"), dict) else _get_nested(evidence, "score_breakdown")
    if not isinstance(sb, dict):
        sb = {}

    # if present in payload
    have = _to_float(payload.get("have", ind.get("have", 0.0)))
    need = _to_float(payload.get("need", ind.get("need", 0.0)))
    ok_soft = _to_float(payload.get("ok_soft", ind.get("ok_soft", 0.0)))

    sc = str(scenario or payload.get("scenario_v4") or ind.get("scenario_v4") or "").lower()
    dir_up = str(direction or payload.get("direction") or ind.get("direction") or "").upper()

    # Align with META_FEAT_V1_COLS construction in core.meta_features_v1
    # We reconstruct almost same features here for training from parquet logs.
    # NOTE: ideally we would use build_meta_features_v1 directly if payload structure matches runtime.
    # But usually parquet logs have slightly different flat structure.
    # For now, we manually map to V1 keys.
    
    # helper for safe extract
    def _g(src, *keys, default=0.0):
        for k in keys:
            if k in src:
                return _to_float(src[k])
        return default

    feat: Dict[str, float] = {
        "base_score": _to_float(sb.get("base_score", ind.get("of_base_score"))),
        "score_final_raw": _to_float(sb.get("final_score_raw", ind.get("of_score_final_raw"))),
        "exec_pen": _to_float(ind.get("exec_pen", evidence.get("exec_pen", 0.0))),
        "have": have,
        "need": need,
        "have_need_ratio": have / max(1.0, need),
        "ok_soft": ok_soft,
        "spread_bps": _to_float(ind.get("spread_bps")),
        "expected_slippage_bps": _to_float(ind.get("expected_slippage_bps")),
        "exec_risk_bps": _to_float(ind.get("exec_risk_bps")),
        "exec_risk_norm": _to_float(ind.get("exec_risk_norm")),
        "exec_risk_ref_bps": _to_float(ind.get("exec_risk_ref_bps")),
        "dir_sign": 1.0 if dir_up in ("LONG", "BUY") else (-1.0 if dir_up in ("SHORT", "SELL") else 0.0),
        "delta_z": _to_float(ind.get("delta_z")),
        "ofi_val": _to_float(ind.get("ofi_val", ind.get("ofi"))),
        "ofi_z": _to_float(ind.get("ofi_z")),
        "ofi_stability_score": _to_float(ind.get("ofi_stability_score")),
        "obi_val": _to_float(ind.get("obi_val", ind.get("obi"))),
        "obi_z": _to_float(ind.get("obi_z")),
        "obi_stable_secs": _to_float(ind.get("obi_stable_secs")),
        "abs_ok": _to_float(ind.get("abs_ok", ind.get("absorption_ok"))),
        "abs_vol": _to_float(ind.get("abs_vol", ind.get("absorption_volume"))),
        "fp_edge_ok": _to_float(ind.get("fp_edge_ok", legs.get("fp_edge_absorb", 0))),
        "fp_edge_strength": _to_float(ind.get("fp_edge_strength")),
        "abs_lvl_score": _to_float(ind.get("abs_lvl_score")),
        "abs_lvl_ladder": _to_float(ind.get("abs_lvl_ladder")),
        "abs_lvl_eff": _to_float(ind.get("abs_lvl_eff")),
        "abs_lvl_poc_edge": _to_float(ind.get("abs_lvl_poc_edge")),
        "data_health": _to_float(ind.get("data_health")),
        "book_health_ok": _to_float(ind.get("book_health_ok")),
        "cvd_quarantine_active": _to_float(ind.get("cvd_quarantine_active")),
        "book_staleness_ms": _to_float(ind.get("book_staleness_ms")),
        "liq_score": _to_float(ind.get("liq_score")),
        "vol_score": _to_float(ind.get("vol_score")),
        "liq_low": 1.0 if "low" in str(ind.get("liq_regime", "")).lower() else 0.0,
        "liq_high": 1.0 if "high" in str(ind.get("liq_regime", "")).lower() else 0.0,
        "vol_low": 1.0 if "low" in str(ind.get("vol_regime", "")).lower() else 0.0,
        "vol_high": 1.0 if "high" in str(ind.get("vol_regime", "")).lower() else 0.0,
        "sc_trend": 1.0 if "trend" in sc else 0.0,
        "sc_range": 1.0 if "range" in sc else 0.0,
        "sc_saw_chop": 1.0 if ("saw" in sc or "chop" in sc) else 0.0,
        "sc_vol_shock": 1.0 if ("vol" in sc or "shock" in sc) else 0.0,
        "sc_reversal": 1.0 if "reversal" in sc else 0.0,
        "sc_continuation": 1.0 if "continuation" in sc else 0.0,
        # Legs (backward compatible)
        "leg_ofi_leg": _to_float(legs.get("ofi_leg", 0)),
        "leg_fp_edge_absorb": _to_float(legs.get("fp_edge_absorb", 0)),
        "leg_obi_stable": _to_float(legs.get("obi_stable", 0)),
        "leg_iceberg_strict": _to_float(legs.get("iceberg_strict", 0)),
        "leg_abs_lvl_ok": _to_float(legs.get("abs_lvl_ok", 0)),
        "leg_reclaim_recent": _to_float(legs.get("reclaim_recent", 0)),
        "leg_weak_progress": _to_float(legs.get("weak_progress", 0)),
        "leg_sweep_recent": _to_float(legs.get("sweep_recent", 0)),
        # V2 Features
        "qimb_l1": _to_float(ind.get("qimb_l1")),
        "qimb_l2": _to_float(ind.get("qimb_l2")),
        "qimb_l3": _to_float(ind.get("qimb_l3")),
        "qimb_l4": _to_float(ind.get("qimb_l4")),
        "qimb_l5": _to_float(ind.get("qimb_l5")),
        "qimb_wmean": _to_float(ind.get("qimb_wmean")),
        "ofi_ml": _to_float(ind.get("ofi_ml")),
        "ofi_ml_wsum": _to_float(ind.get("ofi_ml_wsum")),
        "ofi_ml_norm": _to_float(ind.get("ofi_ml_norm")),
    }
    return feat


def _apply_transforms(df: pd.DataFrame, transforms: Dict[str, List[Dict[str, Any]]]) -> pd.DataFrame:
    out = df.copy()
    for col, ts in transforms.items():
        if col not in out.columns:
            continue
        vals = out[col].to_numpy(dtype=float, copy=True)
        for t in ts:
            # apply_transform is scalar; vectorize
            vals = np.array([apply_transform(float(v), t) for v in vals], dtype=float)
        out[col] = vals
    return out


def _robust_params(x: np.ndarray) -> Dict[str, float]:
    x = x[np.isfinite(x)]
    if x.size == 0:
        return {"center": 0.0, "scale": 1.0}
    med = float(np.median(x))
    mad = float(np.median(np.abs(x - med)))
    if mad <= 1e-9:
        mad = 1.0
    # Consistent with normal: MAD * 1.4826
    scale = float(mad * 1.4826)
    return {"center": med, "scale": scale}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--out_json", required=True)
    ap.add_argument("--out_joblib", default="")
    ap.add_argument("--threshold", type=float, default=0.5)
    ap.add_argument("--C", type=float, default=1.0)
    ap.add_argument("--class_weight", default="balanced")
    ap.add_argument("--schema", default="meta_feat_v1", help="Schema to use: meta_feat_v1 or meta_feat_v2")
    args = ap.parse_args()

    schema_info = SCHEMAS.get(args.schema)
    if not schema_info:
        # Fallback or error? defaulting to v1 if unknown
        print(f"Unknown schema {args.schema}, falling back to meta_feat_v1")
        schema_info = SCHEMAS["meta_feat_v1"]

    FEATURES = schema_info["cols"]
    
    # Resolve transforms: wrap into list if needed
    # We prefer schema transforms, but might fallback to legacy for V1 if needed.
    # For now, let's use the schema's defined transforms as the source of truth.
    # But we need to convert Dict[str, Dict] -> Dict[str, List[Dict]]
    
    transforms_map = {}
    # If using V1, maybe we want to keep using TRANSFORMS_LEGACY to not break existing models?
    # The user request implies moving towards canonical.
    # Let's try to use the definition from the schema module.
    
    schema_transforms = schema_info["transforms"]
    for k, v in schema_transforms.items():
        transforms_map[k] = [v]
    
    # If legacy fallback desired for v1:
    if args.schema == "meta_feat_v1":
        # Merge: use legacy if present, else schema
        # actually legacy covers most.
        pass
    
    # Update global or local variable
    global TRANSFORMS
    TRANSFORMS = transforms_map

    df = pd.read_parquet(args.parquet)
    if "y" not in df.columns:
        raise SystemExit("missing column y")

    # Extract payloads
    payloads = [_get_payload(x) for x in df["indicators"].tolist()] if "indicators" in df.columns else [{}] * len(df)

    # Direction/scenario columns may exist outside indicators
    directions = df["direction"].astype(str).tolist() if "direction" in df.columns else ["" for _ in payloads]
    scenarios = df["scenario_v4"].astype(str).tolist() if "scenario_v4" in df.columns else ["" for _ in payloads]

    rows = []
    for p, d, s in zip(payloads, directions, scenarios):
        feat = _build_features(p, direction=d, scenario=s)
        rows.append(feat)

    Xdf = pd.DataFrame(rows)
    # Ensure all expected cols
    for c in FEATURES:
        if c not in Xdf.columns:
            Xdf[c] = 0.0
    Xdf = Xdf[FEATURES].fillna(0.0)

    y = df["y"].to_numpy()
    mask = np.isfinite(y)
    Xdf = Xdf.loc[mask].reset_index(drop=True)
    y = y[mask].astype(int)

    # Apply transforms and robust scaling
    Xtr = _apply_transforms(Xdf, TRANSFORMS)

    robust_scaler = {"params": {}}
    for c in FEATURES:
        # don't scale obvious binary features
        if c.startswith("leg_") or c.startswith("sc_") or c in ("liq_low", "liq_high", "vol_low", "vol_high", "dir_sign"):
            continue
        robust_scaler["params"][c] = _robust_params(Xtr[c].to_numpy(dtype=float))

    # Scale
    X = Xtr.to_numpy(dtype=float, copy=True)
    for j, c in enumerate(FEATURES):
        p = robust_scaler["params"].get(c)
        if not p:
            continue
        center = float(p.get("center", 0.0))
        scale = float(p.get("scale", 1.0))
        if scale <= 1e-9:
            scale = 1.0
        X[:, j] = (X[:, j] - center) / scale

    cw = None if args.class_weight.lower() == "none" else args.class_weight
    lr = LogisticRegression(
        penalty="l2",
        C=float(args.C),
        solver="liblinear",
        max_iter=500,
        class_weight=cw,
    )
    lr.fit(X, y)

    model_json = {
        "features": FEATURES,
        "intercept": float(lr.intercept_[0]),
        "coef": [float(x) for x in lr.coef_[0].tolist()],
        "threshold": float(args.threshold),
        "transforms": TRANSFORMS, # These are List[Dict] now, but model loader handles Dict[str, Dict] usually... 
        # Wait, MetaModelLR.load expects transforms to be Dict[str, Dict] OR logic to handle it.
        # core/meta_model_lr.py says: transforms=obj.get("transforms", {})
        # And apply logic? 
        # Actually in meta_model_lr.py:
        # self.transforms = transforms
        # predict_proba -> _apply_transforms -> apply_transform(val, t)
        # It expects t to be a Dict.
        # So we should probably store Dict[str, Dict] in JSON, i.e. single transform per feature.
        # But our training code uses List[Dict].
        # We should unwrap if length is 1.
        "robust_scaler": robust_scaler,
        "schema_name": schema_info["name"],
        "schema_version": schema_info["version"],
        "schema_hash": schema_info["hash"],
        "feature_cols_hash": schema_info["hash"], # Requested by user
    }
    
    # Unwrap transforms for JSON if they are single-element lists (canonical compliance)
    json_transforms = {}
    for k, v in TRANSFORMS.items():
        if isinstance(v, list) and len(v) == 1:
            json_transforms[k] = v[0]
        else:
            json_transforms[k] = v
    model_json["transforms"] = json_transforms

    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(model_json, f, ensure_ascii=False, indent=2)

    if args.out_joblib:
        try:
            import joblib  # type: ignore
            joblib.dump(model_json, args.out_joblib)
        except Exception:
            pass


if __name__ == "__main__":
    main()
