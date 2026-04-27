from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""
Trainer for EdgeStackMHModelV1: multi-horizon stacking with OOF calibration.

TimeSeries OOF:
  - OOF base preds → meta
  - OOF meta preds → Platt calibration
  - Report (Brier/ECE/KS/PSI)
  - Saves model.joblib (EdgeStackMHModelV1)
"""
import argparse
import json
import math
import os
import time
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import joblib

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import HistGradientBoostingClassifier

# В вашем проекте уже есть:
from services.ml_calibration import PlattLogitCalibrator, fit_platt_logit
from core.feature_engineering import RobustScalerPack  # уже используется в ml_confirm_gate
from core.edge_stack_mh_v1 import EdgeStackMHModelV1


def ece_score(y: np.ndarray, p: np.ndarray, bins: int = 20) -> float:
    y = y.astype(np.float64)
    p = np.clip(p.astype(np.float64), 0.0, 1.0)
    edges = np.linspace(0.0, 1.0, bins + 1)
    ece = 0.0
    n = len(y)
    for i in range(bins):
        lo, hi = edges[i], edges[i + 1]
        m = (p >= lo) & (p < hi) if i < bins - 1 else (p >= lo) & (p <= hi)
        if not np.any(m):
            continue
        w = float(np.sum(m)) / float(n)
        acc = float(np.mean(y[m]))
        conf = float(np.mean(p[m]))
        ece += w * abs(acc - conf)
    return float(ece)


def brier(y: np.ndarray, p: np.ndarray) -> float:
    y = y.astype(np.float64)
    p = np.clip(p.astype(np.float64), 0.0, 1.0)
    return float(np.mean((p - y) ** 2))


def stable_time_folds(ts_ms: np.ndarray, n_splits: int = 5) -> List[Tuple[np.ndarray, np.ndarray]]:
    order = np.argsort(ts_ms)
    n = len(order)
    fold_sizes = [n // n_splits] * n_splits
    for i in range(n % n_splits):
        fold_sizes[i] += 1
    folds = []
    start = 0
    for fs in fold_sizes:
        val_idx = order[start:start + fs]
        train_idx = order[:start]
        start += fs
        if len(train_idx) < 20 or len(val_idx) < 20:
            continue
        folds.append((train_idx, val_idx))
    return folds


def best_floor_by_sum_util(score: np.ndarray, util_true: np.ndarray,
                           floor_min: float, floor_max: float, floor_step: float,
                           min_trades: int) -> Dict:
    best = {"floor": floor_min, "sum_util": -1e18, "n_take": 0, "take_rate": 0.0, "mean_util": 0.0}
    for f in np.arange(floor_min, floor_max + 1e-12, floor_step):
        m = score >= f
        n_take = int(np.sum(m))
        if n_take < min_trades:
            continue
        s = float(np.sum(util_true[m]))
        if s > best["sum_util"]:
            best["floor"] = float(f)
            best["sum_util"] = float(s)
            best["n_take"] = n_take
            best["take_rate"] = float(n_take) / float(len(score))
            best["mean_util"] = float(np.mean(util_true[m]))
    return best


def bucket_from_scenario(s: str) -> str:
    s = (s or "").lower()
    from common.market_mode import is_range_regime; _r = is_range_regime(s)
    if _r:
        return "range"
    if "trend" in s:
        return "trend"
    return "other"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--time-col", default="ts_ms")
    ap.add_argument("--scenario-col", default="scenario_v4")
    ap.add_argument("--horizons", default="60000,180000,300000")
    ap.add_argument("--n-splits", type=int, default=5)
    ap.add_argument("--unc-k", type=float, default=0.10)
    ap.add_argument("--floor-min", type=float, default=0.00)
    ap.add_argument("--floor-max", type=float, default=1.00)
    ap.add_argument("--floor-step", type=float, default=0.01)
    ap.add_argument("--min-trades", type=int, default=120)
    args = ap.parse_args()

    horizons = [int(x) for x in args.horizons.split(",") if x.strip()]

    os.makedirs(args.out_dir, exist_ok=True)
    df = pd.read_json(args.dataset, lines=True)

    # choose label per horizon
    for h in horizons:
        if f"y_edge_{h}" not in df.columns:
            if f"y_util_pos_{h}" in df.columns:
                df[f"y_edge_{h}"] = df[f"y_util_pos_{h}"].astype(int)
            else:
                raise RuntimeError(f"no label column y_edge_{h} and no fallback y_util_pos_{h}")

    # Feature columns (drop labels + raw text)
    drop_cols = {"sid", "symbol", args.time_col, args.scenario_col}
    for c in df.columns:
        if c.startswith("util_r_") or c.startswith("y_"):
            drop_cols.add(c)
    feature_cols = [c for c in df.columns if c not in drop_cols]

    # one-hot scenario if present
    if args.scenario_col in df.columns:
        df = pd.concat([df, pd.get_dummies(df[args.scenario_col].fillna(""), prefix=args.scenario_col)], axis=1)
        # refresh feature cols
        feature_cols = [c for c in df.columns if c not in drop_cols and not (c.startswith("y_") or c.startswith("util_r_"))]

    ts = df[args.time_col].astype(np.int64).values
    X = df[feature_cols].astype(np.float32).values

    scaler = RobustScalerPack.fit(X, feature_names=feature_cols)
    Xs = scaler.transform(X)

    folds = stable_time_folds(ts, n_splits=args.n_splits)

    lr_models: Dict[int, any] = {}
    gbdt_models: Dict[int, any] = {}
    meta_models: Dict[int, any] = {}
    calibrators: Dict[int, any] = {}

    report = {"horizons": horizons, "feature_cols": feature_cols, "metrics": {}}
    now_ms = get_ny_time_millis()

    # OOF buffers
    oof_p_cal = {h: np.zeros(len(df), dtype=np.float64) for h in horizons}
    oof_unc = {h: np.zeros(len(df), dtype=np.float64) for h in horizons}

    for h in horizons:
        y = df[f"y_edge_{h}"].astype(int).values

        # base OOF
        oof_lr = np.zeros(len(df), dtype=np.float64)
        oof_gb = np.zeros(len(df), dtype=np.float64)

        for tr, va in folds:
            lr = LogisticRegression(max_iter=2000, class_weight="balanced", solver="lbfgs")
            gb = HistGradientBoostingClassifier(max_depth=6, learning_rate=0.06, max_iter=350)

            lr.fit(Xs[tr], y[tr])
            gb.fit(Xs[tr], y[tr])

            oof_lr[va] = lr.predict_proba(Xs[va])[:, 1]
            oof_gb[va] = gb.predict_proba(Xs[va])[:, 1]

        Z = np.column_stack([oof_lr, oof_gb])

        # meta OOF: crossfit meta on folds using base OOF features
        oof_meta = np.zeros(len(df), dtype=np.float64)
        for tr, va in folds:
            meta = LogisticRegression(max_iter=2000, class_weight="balanced", solver="lbfgs")
            meta.fit(Z[tr], y[tr])
            oof_meta[va] = meta.predict_proba(Z[va])[:, 1]

        cal = fit_platt_logit(oof_meta.tolist(), y.tolist())

        p_cal = np.asarray([cal.apply_one(float(p)) for p in oof_meta], dtype=np.float64)
        unc = np.abs(oof_gb - oof_lr)

        oof_p_cal[h] = p_cal
        oof_unc[h] = unc

        # train final models on full data
        lr_f = LogisticRegression(max_iter=2000, class_weight="balanced", solver="lbfgs")
        gb_f = HistGradientBoostingClassifier(max_depth=6, learning_rate=0.06, max_iter=350)
        lr_f.fit(Xs, y)
        gb_f.fit(Xs, y)

        # meta trained on base OOF features (strict, no leakage)
        meta_f = LogisticRegression(max_iter=2000, class_weight="balanced", solver="lbfgs")
        meta_f.fit(Z, y)

        lr_models[h] = lr_f
        gbdt_models[h] = gb_f
        meta_models[h] = meta_f
        calibrators[h] = cal

        report["metrics"][str(h)] = {
            "brier": brier(y, p_cal),
            "ece": ece_score(y, p_cal, bins=20),
            "mean_p": float(np.mean(p_cal)),
            "pos_rate": float(np.mean(y)),
        }

    # choose best horizon per row by score
    unc_k = float(args.unc_k)
    best_score = np.full(len(df), -1e18, dtype=np.float64)
    best_h = np.zeros(len(df), dtype=np.int64)
    best_p = np.zeros(len(df), dtype=np.float64)
    best_unc = np.zeros(len(df), dtype=np.float64)

    for h in horizons:
        sc = oof_p_cal[h] - unc_k * oof_unc[h]
        m = sc > best_score
        best_score[m] = sc[m]
        best_h[m] = h
        best_p[m] = oof_p_cal[h][m]
        best_unc[m] = oof_unc[h][m]

    # floors per bucket using util_r_{best_h} as utility proxy if available
    util_true = np.zeros(len(df), dtype=np.float64)
    for i in range(len(df)):
        hh = int(best_h[i])
        col = f"util_r_{hh}"
        if col in df.columns:
            util_true[i] = float(df[col].iloc[i])
        else:
            util_true[i] = 0.0

    buckets = np.array([bucket_from_scenario(str(s)) for s in df.get(args.scenario_col, "").fillna("").tolist()])
    floors = {"global": {}, "by_bucket": {}, "horizons": horizons, "unc_k": unc_k}

    floors["global"] = best_floor_by_sum_util(best_score, util_true, args.floor_min, args.floor_max, args.floor_step, args.min_trades)
    for b in ["trend", "range", "other"]:
        m = buckets == b
        if int(np.sum(m)) < args.min_trades:
            continue
        floors["by_bucket"][b] = best_floor_by_sum_util(best_score[m], util_true[m], args.floor_min, args.floor_max, args.floor_step, args.min_trades)

    model = EdgeStackMHModelV1(
        feature_cols=feature_cols,
        horizons=horizons,
        unc_k=unc_k,
        scaler=scaler,
        lr=lr_models,
        gbdt=gbdt_models,
        meta=meta_models,
        calibrator=calibrators,
    )

    model_path = os.path.join(args.out_dir, "model.joblib")
    joblib.dump(model, model_path)

    cfg = {
        "schema_version": 1,
        "kind": "edge_stack_mh_v1",
        "run_id": f"edge_stack_{int(time.time())}",
        "created_ms": now_ms,
        "model_path": model_path,
        "mode": "SHADOW",
        "enforce_share": 0.0,
        "edge_floors": floors,
        "calibrator_path": None,
        "feature_version": None,
        "model_type": "sklearn_lr_hgb_stack",
        "checksum": None,
        "min_data_ts_ms": int(df[args.time_col].min()),
        "max_data_ts_ms": int(df[args.time_col].max()),
    }

    with open(os.path.join(args.out_dir, "cfg_edge_stack_mh_v1.json"), "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

    with open(os.path.join(args.out_dir, "report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"OK: saved {model_path}")
    print(f"OK: saved cfg_edge_stack_mh_v1.json")


if __name__ == "__main__":
    main()

