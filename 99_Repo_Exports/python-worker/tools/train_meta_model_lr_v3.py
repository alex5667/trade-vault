#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

from core.meta_features_v1 import (
    META_FEAT_V1_COLS,
    META_FEAT_V1_HASH,
    META_FEAT_V1_NAME,
    META_FEAT_V1_TRANSFORMS,
    META_FEAT_V1_VERSION,
    build_meta_features_v1,
)
from core.meta_model_lr import MetaModelLR
from core.feature_engineering import apply_transform


def robust_center_scale(x: np.ndarray) -> Tuple[float, float]:
    # median / MAD (scaled)
    med = float(np.nanmedian(x))
    mad = float(np.nanmedian(np.abs(x - med)))
    scale = mad * 1.4826
    if (not math.isfinite(scale)) or scale < 1e-9:
        scale = 1.0
    if not math.isfinite(med):
        med = 0.0
    return med, scale


def build_row_features(row: Dict[str, Any]) -> Dict[str, float]:
    have = int(row.get("have", 0) or 0)
    need = int(row.get("need", 0) or 0)
    ok_soft = int(row.get("ok_soft", 0) or 0)
    rule_score = float(row.get("score_final_01", row.get("rule_score", 0.0)) or 0.0)
    exec_risk_norm = float(row.get("exec_risk_norm", 0.0) or 0.0)
    exec_risk_bps = float(row.get("exec_risk_bps", 0.0) or 0.0)
    ml_scenario = str(row.get("scenario_v4", row.get("ml_scenario", "")) or "")

    feat, _missing_raw = build_meta_features_v1(
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-data", required=True, help="Input parquet or csv file")
    ap.add_argument("--label-col", required=True, help="binary label column name")
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--C", type=float, default=1.0)
    ap.add_argument("--max-iter", type=int, default=2000)
    ap.add_argument("--self-check", type=int, default=1, help="1=enable train==serve check")
    ap.add_argument("--self-check-n", type=int, default=2000)
    args = ap.parse_args()

    df = pd.read_parquet(args.in_parquet)
    if args.label_col not in df.columns:
        raise SystemExit(f"label_col '{args.label_col}' not found in parquet columns")

    y = df[args.label_col].astype(int).to_numpy()
    records = df.to_dict(orient="records")

    n = len(records)
    m = len(META_FEAT_V1_COLS)

    feat_dicts: List[Dict[str, float]] = []
    X_raw = np.zeros((n, m), dtype=float)

    for i, row in enumerate(records):
        feat = build_row_features(row)
        feat_dicts.append(feat)
        for j, name in enumerate(META_FEAT_V1_COLS):
            X_raw[i, j] = float(feat.get(name, 0.0) or 0.0)

    # Apply transforms
    X_tf = X_raw.copy()
    for j, name in enumerate(META_FEAT_V1_COLS):
        spec = META_FEAT_V1_TRANSFORMS.get(name)
        if spec:
            # vectorized transform
            X_tf[:, j] = np.vectorize(lambda v: apply_transform(float(v), spec))(X_tf[:, j])

    # Robust scaling
    centers = np.zeros(m, dtype=float)
    scales = np.ones(m, dtype=float)
    for j in range(m):
        c, s = robust_center_scale(X_tf[:, j])
        centers[j] = c
        scales[j] = s
    X = (X_tf - centers) / scales

    lr = LogisticRegression(
        C=float(args.C),
        max_iter=int(args.max_iter),
        class_weight="balanced",
        solver="lbfgs",
    )
    lr.fit(X, y)

    robust_scaler = {
        name: {"center": float(centers[j]), "scale": float(scales[j])}
        for j, name in enumerate(META_FEAT_V1_COLS)
    }

    model_json = {
        "schema_name": META_FEAT_V1_NAME,
        "schema_version": int(META_FEAT_V1_VERSION),
        "schema_hash": str(META_FEAT_V1_HASH),
        "features": list(META_FEAT_V1_COLS),
        "intercept": float(lr.intercept_[0]),
        "coef": [float(x) for x in lr.coef_[0].tolist()],
        "threshold": 0.5,
        "transforms": dict(META_FEAT_V1_TRANSFORMS),
        "robust_scaler": robust_scaler,
    }

    # Ensure output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(args.out_json)), exist_ok=True)

    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(model_json, f, ensure_ascii=False, indent=2)

    # Train==Serve self-check (key guarantee)
    if int(args.self_check) == 1:
        mm = MetaModelLR.load(args.out_json)
        k = min(int(args.self_check_n), n)
        # Runtime prediction loop
        p_rt = np.array([mm.predict_proba(feat_dicts[i]) for i in range(k)], dtype=float)
        # Sklearn prediction
        p_sk = lr.predict_proba(X[:k])[:, 1]
        err = float(np.max(np.abs(p_rt - p_sk)))
        if err > 1e-6:
            print(f"ERROR: Train==Serve mismatch: max_abs_err={err:.3e} > 1e-6")
            # Log first mismatch details
            idx = np.argmax(np.abs(p_rt - p_sk))
            print(f"Mismatch at index {idx}: runtime={p_rt[idx]:.6f}, sklearn={p_sk[idx]:.6f}")
            raise SystemExit(1)
        else:
            print(f"Self-check PASSED: max_abs_err={err:.3e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
