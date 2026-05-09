"""
Train Confidence Calibrator V2 (Universal Schema)

Supports:
- Methods: Platt (Logistic), Isotonic, Beta, Auto (Model Selection)
- Input: JSONL (y, indicators.confidence, context...)
- Output: Schema V3 Bundle (Hierarchical Buckets)
- Metrics: ECE, Brier
- Hierarchical Bucketing: Generates parent buckets for fallback.
- Time-based Split: Uses ts_ms for train/val split.

Usage:
  python3 -m ml_analysis.tools.train_confidence_calibrator_v2 \
    --in_jsonl /path/to/data.jsonl \
    --out_bundle /path/to/bundle_v2.json \
    --method auto \
    --bucket_by session_regime
"""

import argparse
import json
import logging
import os
import sys
import time
from typing import Any

import numpy as np

from ml_analysis.calibration_extended import report as extended_calibration_report

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("conf_cal_train_v2")

# -------------------------------------------------------------------------
# Math / Calibration Primitives
# -------------------------------------------------------------------------

def _clamp01(p: float, eps: float = 1e-6) -> float:
    return max(eps, min(1.0 - eps, p))

def _logit(p: np.ndarray, eps: float = 1e-6) -> np.ndarray:
    p = np.clip(p, eps, 1.0 - eps)
    return np.log(p / (1.0 - p))

def _sigmoid(z: np.ndarray) -> np.ndarray:
    out = np.empty_like(z, dtype=np.float64)
    mask = (z >= 0)
    out[mask] = 1.0 / (1.0 + np.exp(-z[mask]))
    z_neg = z[~mask]
    exp_z = np.exp(z_neg)
    out[~mask] = exp_z / (1.0 + exp_z)
    return out

def fit_platt(y: np.ndarray, p: np.ndarray, eps: float = 1e-6) -> dict[str, float]:
    """
    Fits Platt Scaling (Logistic Regression on logit(p)).
    Model: logit(p_cal) = a * logit(p) + b
    """
    if len(y) < 10:
        return {"a": 1.0, "b": 0.0}

    z = _logit(p, eps)

    # Newton-Raphson for Logistic Regression
    a = 1.0
    b = 0.0

    for _ in range(100):
        s = a * z + b
        q = _sigmoid(s)
        diff = q - y
        grad_a = np.mean(diff * z)
        grad_b = np.mean(diff)

        w = q * (1.0 - q)
        h_aa = np.mean(w * z * z) + 1e-9
        h_ab = np.mean(w * z)
        h_bb = np.mean(w) + 1e-9

        det = h_aa * h_bb - h_ab * h_ab
        if det < 1e-12: break

        da = (h_bb * grad_a - h_ab * grad_b) / det
        db = (-h_ab * grad_a + h_aa * grad_b) / det

        a -= da
        b -= db

        if abs(da) < 1e-6 and abs(db) < 1e-6:
            break

    return {"a": float(a), "b": float(b)}

def fit_beta(y: np.ndarray, p: np.ndarray, eps: float = 1e-6) -> dict[str, float]:
    """
    Fits Beta Calibration.
    Model: logit(p_cal) = a * ln(p) + b * ln(1-p) + c
    """
    if len(y) < 10:
        return {"a": 1.0, "b": 1.0, "c": 0.0}

    p_clipped = np.clip(p, eps, 1.0 - eps)
    ln_p = np.log(p_clipped)
    ln_1_p = np.log(1.0 - p_clipped)

    X = np.column_stack((ln_p, ln_1_p, np.ones_like(p)))
    W = np.array([1.0, -1.0, 0.0])

    for _ in range(100):
        s = X @ W
        q = _sigmoid(s)
        diff = q - y

        grad = (X.T @ diff) / len(y)
        w = q * (1.0 - q)
        WX = X * w[:, None]
        H = (X.T @ WX) / len(y)
        H += np.eye(3) * 1e-9

        try:
            delta = np.linalg.solve(H, grad)
        except np.linalg.LinAlgError:
            break

        W -= delta
        if np.max(np.abs(delta)) < 1e-6:
            break

    return {"a": float(W[0]), "b": float(W[1]), "c": float(W[2])}

def fit_isotonic(y: np.ndarray, p: np.ndarray) -> dict[str, list[float]]:
    """
    Fits Isotonic Regression using sklearn.
    """
    try:
        from sklearn.isotonic import IsotonicRegression
        iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
        iso.fit(p, y)
        return {"boundaries": iso.X_thresholds_.tolist(), "values": iso.y_thresholds_.tolist()}
    except ImportError:
        logger.warning("sklearn not found - isotonic unavailable")
        return {"boundaries": [], "values": []}

def apply_calibration(p: np.ndarray, method: str, params: dict[str, Any], eps: float = 1e-6) -> np.ndarray:
    if method == "identity":
        return p
    elif method == "platt" or method == "platt_logit":
        a, b = params.get("a", 1.0), params.get("b", 0.0)
        z = _logit(p, eps)
        return _sigmoid(a * z + b)
    elif method in ("beta", "beta_simplified"):
        a, b, c = params.get("a", 1.0), params.get("b", 1.0), params.get("c", 0.0)
        p_clipped = np.clip(p, eps, 1.0 - eps)
        logit = a * np.log(p_clipped) + b * np.log(1.0 - p_clipped) + c
        return _sigmoid(logit)
    elif method == "isotonic":
        boundaries = np.array(params.get("boundaries", []))
        values = np.array(params.get("values", []))
        if len(boundaries) == 0:
            return p
        return np.interp(p, boundaries, values)
    return p

# -------------------------------------------------------------------------
# Metrics
# -------------------------------------------------------------------------

def calc_ece(y: np.ndarray, p: np.ndarray, bins: int = 15) -> float:
    return float(extended_calibration_report(y, p, bins=bins).get("ece", 0.0) or 0.0)


def calc_brier(y: np.ndarray, p: np.ndarray) -> float:
    return float(extended_calibration_report(y, p).get("brier", 0.0) or 0.0)

# -------------------------------------------------------------------------
# Data Loading & Hierarchy
# -------------------------------------------------------------------------

def get_hierarchical_keys(context: dict[str, Any], s_ver: int = 3) -> list[str]:
    """
    Generates list of bucket keys for a given context.
    Hierarchical: SYM|sess|reg -> SYM|sess|any -> SYM|any|reg -> SYM|any|any -> GLOBAL
    """
    keys = ["global"]

    s = (context.get("session", "OFF"))
    r = (context.get("regime", "neutral"))
    sym = (context.get("symbol", "unknown"))

    if s_ver >= 3:
        keys.append(f"{sym}|any|any")
        keys.append(f"{sym}|any|{r}")
        keys.append(f"{sym}|{s}|any")
        keys.append(f"{sym}|{s}|{r}")
        # Global variants if needed
        keys.append(f"GLOBAL|any|{r}")
        keys.append(f"GLOBAL|{s}|any")
        keys.append(f"GLOBAL|{s}|{r}")

    return keys

def load_data_hierarchical(
    jsonl_path: str,
    key: str,
    hierarchical: bool = True,
    min_rows: int = 100
) -> dict[str, dict[str, list]]:
    """
    Loads data and populates multiple buckets per row.
    Returns: { bucket_key: { "y": [], "p": [], "ts": [] } }
    """
    data_buckets = {}

    count = 0
    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            try:
                row = json.loads(line)
            except Exception: continue

            y_val = row.get("y")
            if y_val is None: continue
            y = int(y_val)

            # Confidence
            indicators = row.get("indicators", {})
            raw = indicators.get(key)
            if raw is None: raw = row.get(key)
            if raw is None: continue
            try: p_val = float(raw)
            except Exception: continue

            # Timestamp
            ts = row.get("ts_ms") or row.get("ts") or 0

            # Context
            ctx = row.get("context", row)

            # Identify keys
            bucket_keys = ["global"]
            if hierarchical:
                bucket_keys = get_hierarchical_keys(ctx, s_ver=3)

            # Distribute
            for bk in bucket_keys:
                if bk not in data_buckets:
                    data_buckets[bk] = {"y": [], "p": [], "ts": []}
                data_buckets[bk]["y"].append(y)
                data_buckets[bk]["p"].append(p_val)
                data_buckets[bk]["ts"].append(ts)

            count += 1

    logger.info(f"Loaded {count} rows. Generated {len(data_buckets)} buckets.")
    return data_buckets

# -------------------------------------------------------------------------
# Training & Selection
# -------------------------------------------------------------------------

def fit_and_select(
    y: list[int], p: list[float], ts: list[int],
    method_arg: str,
    min_rows: int = 100
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Splits by time (if possible), trains candidates, selects best (if auto).
    Returns (best_params, report)
    """
    n = len(y)
    if n < min_rows:
        return {}, {"method": "identity", "n": n, "note": "insufficient_data"}

    y_np = np.array(y, dtype=np.float64)
    p_np = np.array(p, dtype=np.float64)
    ts_np = np.array(ts, dtype=np.int64)

    # Sort by time
    sort_idx = np.argsort(ts_np)
    y_sorted = y_np[sort_idx]
    p_sorted = p_np[sort_idx]

    # Split
    split_idx = int(n * 0.8)
    y_train, y_val = y_sorted[:split_idx], y_sorted[split_idx:]
    p_train, p_val = p_sorted[:split_idx], p_sorted[split_idx:]

    candidates = []
    if method_arg == "auto":
        candidates = ["identity", "platt_logit", "beta"]
        # Only try isotonic if decent size to avoid overfitting
        if len(y_train) > 500:
            candidates.append("isotonic")
    else:
        # Map argument 'platt' to 'platt_logit' internally if user asks for platt
        if method_arg == "platt":
            candidates = ["platt_logit"]
        else:
            candidates = [method_arg]

    best_method = "identity"
    best_ece = 999.0
    best_brier = 999.0

    results = {}

    for m in candidates:
        # Fit on Train
        params = {}
        if m == "platt_logit": params = fit_platt(y_train, p_train)
        elif m == "beta": params = fit_beta(y_train, p_train)
        elif m == "isotonic": params = fit_isotonic(y_train, p_train)

        # Eval on Val
        # If val is empty (edge case), use train
        eval_y, eval_p = (y_val, p_val) if len(y_val) > 0 else (y_train, p_train)

        cal_p = apply_calibration(eval_p, m, params)
        rep_eval = extended_calibration_report(eval_y, cal_p, bins=15)
        ece = float(rep_eval.get("ece", 0.0) or 0.0)
        brier = float(rep_eval.get("brier", 0.0) or 0.0)

        results[m] = {
            "ece": ece,
            "mce": float(rep_eval.get("mce", 0.0) or 0.0),
            "brier": brier,
            "calibration_slope": float(rep_eval.get("calibration_slope", float("nan"))),
            "calibration_intercept": float(rep_eval.get("calibration_intercept", float("nan"))),
            "sharpness_mean": float(rep_eval.get("sharpness_mean", float("nan"))),
            "sharpness_entropy": float(rep_eval.get("sharpness_entropy", float("nan"))),
            "prob_mass_near_half": float(rep_eval.get("prob_mass_near_half", float("nan"))),
        }

        # Selection Logic
        # Prefer lower ECE. Tie-break Brier.
        if ece < best_ece - 1e-4: # meaningful improvement
            best_ece = ece
            best_brier = brier
            best_method = m
        elif abs(ece - best_ece) < 1e-4:
            if brier < best_brier:
                best_brier = brier
                best_method = m

    # Final Fit on ALL data using Best Method
    final_params = {}
    if best_method == "platt_logit": final_params = fit_platt(y_sorted, p_sorted)
    elif best_method == "beta": final_params = fit_beta(y_sorted, p_sorted)
    elif best_method == "isotonic": final_params = fit_isotonic(y_sorted, p_sorted)


    # Final Report (Val performance of selected method, and Raw)
    # Validate selected on Val
    val_y_final = y_val if len(y_val) > 0 else y_train
    val_p_final = p_val if len(y_val) > 0 else y_train

    cal_val_p = apply_calibration(val_p_final, best_method, final_params)

    raw_ext = extended_calibration_report(val_y_final, val_p_final, bins=15)
    cal_ext = extended_calibration_report(val_y_final, cal_val_p, bins=15)
    raw_ece = float(raw_ext.get("ece", 0.0) or 0.0)
    raw_brier = float(raw_ext.get("brier", 0.0) or 0.0)
    final_ece = float(cal_ext.get("ece", 0.0) or 0.0)
    final_brier = float(cal_ext.get("brier", 0.0) or 0.0)

    report = {
        "n": n,
        "n_train": len(y_train),
        "n_val": len(y_val),
        "method_selected": best_method,
        "candidates": results,
        "raw": {
            **raw_ext,
            "ece": raw_ece,
            "brier": raw_brier,
            "mean_conf": float(np.mean(val_p_final)),
            "accuracy": float(np.mean(val_y_final))
        },
        "cal": {
            **cal_ext,
            "ece": final_ece,
            "brier": final_brier
        }
    }

    return final_params, report

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in_jsonl", required=True)
    parser.add_argument("--out_bundle", required=True)
    parser.add_argument("--key", default="confidence_v1")
    parser.add_argument("--method", default="auto", help="Method: auto, platt, isotonic, beta, identity")
    parser.add_argument("--hierarchical", type=int, default=1, help="Enable hierarchical bucketing")
    parser.add_argument("--min_rows", type=int, default=100)
    args = parser.parse_args()

    if not os.path.exists(args.in_jsonl):
        logger.error(f"Input file not found: {args.in_jsonl}")
        sys.exit(1)

    # 1. Load Data (Hierarchical)
    hier = bool(args.hierarchical)
    buckets_data = load_data_hierarchical(args.in_jsonl, args.key, hierarchical=hier, min_rows=args.min_rows)

    # 2. Train Bundle
    bundle = {
        "schema_version": 3,
        "input_key": args.key,
        "generated_at": time.time(),
        "meta": {
            "method_global": "auto" if args.method == "auto" else args.method,
            "training_source": args.in_jsonl,
            "hierarchical": hier
        },
        "buckets": {},
        "train_report": {}
    }

    # Train Global First
    g_data = buckets_data.get("global")
    global_report = {}

    if g_data:
        logger.info(f"Training GLOBAL on {len(g_data['y'])} rows...")
        params, report = fit_and_select(g_data["y"], g_data["p"], g_data["ts"], args.method, args.min_rows)
        bundle["buckets"]["global"] = {
            "method": report["method_selected"],
            "params": params,
            "metrics": report
        }
        global_report = report
    else:
        logger.warning("No global data.")

    # Train other buckets
    for bkey, bdata in buckets_data.items():
        if bkey == "global": continue
        if len(bdata["y"]) < args.min_rows: continue

        # Optimization: if hierarchical, maybe strict check?

        params, report = fit_and_select(bdata["y"], bdata["p"], bdata["ts"], args.method, args.min_rows)
        bundle["buckets"][bkey] = {
            "method": report["method_selected"],
            "params": params,
            "metrics": report
        }

    # Attach global report to top level for nightly operator
    bundle["train_report"] = global_report

    # 3. Save
    tmp_path = args.out_bundle + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2)
    os.rename(tmp_path, args.out_bundle)

    logger.info(f"Saved bundle to {args.out_bundle}. Buckets: {len(bundle['buckets'])}")

    if global_report:
        raw = global_report.get("raw", {})
        cal = global_report.get("cal", {})
        print(f"Global ECE: {raw.get('ece',0):.4f} -> {cal.get('ece',0):.4f}")

if __name__ == "__main__":
    main()
