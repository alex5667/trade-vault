"""Evaluate meta model thresholds for ENFORCE mode.

Evaluates different meta_p_min thresholds on a dataset to find the best one
that maximizes meanR while controlling tail loss rate.

Usage:
  python -m tools.eval_meta_enforce \
    --dataset /tmp/dataset.ndjson \
    --model /tmp/meta_lr.json \
    --out /tmp/eval.json \
    --grid 0.50,0.55,0.60,0.65,0.70 \
    --min-pass-rate 0.25 \
    --tail-max 0.18 \
    --tail-improve-min 0.02 \
    --meanr-drop-max 0.05
"""

from __future__ import annotations

import argparse
import json
import math
from typing import Any, Dict, Iterator, List, Tuple


def iter_ndjson(path: str) -> Iterator[Dict[str, Any]]:
    """Iterate over NDJSON file, yielding one dict per line."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def _f(x: Any, d: float = 0.0) -> float:
    """Safe float conversion."""
    try:
        return float(x)
    except Exception:
        return float(d)


def _i(x: Any, d: int = 0) -> int:
    """Safe int conversion."""
    try:
        return int(x)
    except Exception:
        return int(d)


def sigmoid(x: float) -> float:
    """Sigmoid function: 1 / (1 + exp(-x))."""
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def load_lr_model(path: str) -> Dict[str, Any]:
    """Load logistic regression model from JSON file.
    
    Expected format:
    {
        "kind": "logreg_v1",
        "features": ["feat1", "feat2", ...],
        "coef": [w1, w2, ...],
        "intercept": b,
        "threshold": 0.5  # optional
    }
    """
    d = json.loads(open(path, "r", encoding="utf-8").read())
    if d.get("kind") != "logreg_v1":
        raise ValueError(f"unexpected model kind: {d.get('kind')}")
    feats = list(d["features"])
    coef = [float(x) for x in d["coef"]]
    if len(coef) != len(feats):
        raise ValueError("coef/features length mismatch")
    return {
        "features": feats,
        "intercept": float(d["intercept"]),
        "coef": coef,
        "threshold": float(d.get("threshold", 0.5)),
    }


def predict_p(model: Dict[str, Any], row: Dict[str, Any]) -> float:
    """Predict probability using logistic regression model.
    
    Args:
        model: Model dict with features, coef, intercept
        row: Data row with feature values
        
    Returns:
        Predicted probability (0.0 to 1.0)
    """
    s = float(model["intercept"])
    feats = model["features"]
    coef = model["coef"]
    for name, w in zip(feats, coef):
        s += float(w) * _f(row.get(name, 0.0), 0.0)
    return float(sigmoid(s))


def metrics(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    """Compute outcome metrics from rows.
    
    Args:
        rows: List of rows with r_mult field
        
    Returns:
        Dict with n, meanR, tail_rate
    """
    n = len(rows)
    if n == 0:
        return {"n": 0}
    sum_r = 0.0
    tail = 0
    for r in rows:
        rm = _f(r.get("r_mult", 0.0), 0.0)
        sum_r += rm
        if rm <= -1.0:
            tail += 1
    return {
        "n": float(n),
        "meanR": float(sum_r / n),
        "tail_rate": float(tail / n),
    }


def main() -> None:
    """Main entry point: evaluate thresholds and find best meta_p_min."""
    ap = argparse.ArgumentParser(description="Evaluate meta model thresholds for ENFORCE")
    ap.add_argument("--dataset", required=True, help="NDJSON from build_of_dataset.py")
    ap.add_argument("--model", required=True, help="meta LR model json (logreg_v1)")
    ap.add_argument("--out", required=True, help="output JSON decision")
    ap.add_argument("--grid", default="0.50,0.55,0.60,0.65,0.70", help="comma-separated threshold grid")
    ap.add_argument("--min-pass-rate", type=float, default=0.25, help="min share of baseline ok-trades after filter")
    ap.add_argument("--tail-max", type=float, default=0.18, help="max tail loss rate in filtered set")
    ap.add_argument("--tail-improve-min", type=float, default=0.02, help="require at least this tail-rate improvement vs baseline")
    ap.add_argument("--meanr-drop-max", type=float, default=0.05, help="max allowed meanR drop vs baseline")
    ap.add_argument("--only-ok", type=int, default=1, help="evaluate only rows where rule ok==1")
    args = ap.parse_args()

    model = load_lr_model(args.model)
    grid = [float(x) for x in args.grid.split(",") if x.strip()]

    # Load dataset
    all_rows = []
    for r in iter_ndjson(args.dataset):
        if args.only_ok == 1 and _i(r.get("ok", 0), 0) != 1:
            continue
        all_rows.append(r)

    base = metrics(all_rows)
    if base["n"] <= 0:
        raise SystemExit("no_rows_for_eval (ok==1 filter too strict or dataset empty)")

    best = None
    best_obj = -1e9

    for th in grid:
        kept = []
        for r in all_rows:
            p = predict_p(model, r)
            # store for later analysis if needed
            # r["_meta_p"] = p
            if p >= th:
                kept.append(r)

        m = metrics(kept)
        if m["n"] <= 0:
            continue

        pass_rate = float(m["n"] / base["n"])
        tail_improve = float(base["tail_rate"] - m["tail_rate"])
        meanr_drop = float(base["meanR"] - m["meanR"])

        # constraints
        if pass_rate < args.min_pass_rate:
            continue
        if m["tail_rate"] > args.tail_max:
            continue
        if tail_improve < args.tail_improve_min:
            continue
        if meanr_drop > args.meanr_drop_max:
            continue

        # objective: maximize meanR, penalize tail
        obj = float(m["meanR"] - 0.50 * m["tail_rate"])
        if obj > best_obj:
            best_obj = obj
            best = {
                "meta_p_min": th,
                "baseline": {
                    "n": int(base["n"]),
                    "meanR": base["meanR"],
                    "tail_rate": base["tail_rate"],
                },
                "filtered": {
                    "n": int(m["n"]),
                    "pass_rate": pass_rate,
                    "meanR": m["meanR"],
                    "tail_rate": m["tail_rate"],
                },
                "delta": {
                    "tail_improve": tail_improve,
                    "meanR_drop": meanr_drop,
                },
                "objective": obj,
            }

    out = {"best": best, "grid": grid}
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    if best is None:
        print("no_valid_threshold")
    else:
        print(json.dumps(best, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

