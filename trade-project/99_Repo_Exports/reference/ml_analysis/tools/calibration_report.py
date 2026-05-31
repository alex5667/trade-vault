"""Calibration report for raw vs calibrated confidences.

Computes:
  - ECE / MCE
  - Brier
  - Calibration slope / intercept
  - Precision@Top5%
  - Expectancy R @Top5%
  - Sharpness mean / entropy
  - Probability mass near 0.5
"""
from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List

import numpy as np

from ml_analysis.calibration_extended import report as extended_report


def _get(ind: Dict[str, Any], keys: List[str]) -> float | None:
    for k in keys:
        v = ind.get(k)
        if v is None:
            continue
        try:
            f = float(v)
            if np.isfinite(f):
                return float(max(0.0, min(1.0, f)))
        except Exception:
            continue
    return None


def load(path: str) -> Dict[str, np.ndarray]:
    y, r = [], []
    p_v1, p_v2, c_v1, c_v2 = [], [], [], []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            ind = row.get("indicators") or {}
            yy = int(row.get("y", 0) or 0)
            rr = row.get("r_mult", None)
            rr = float(rr) if rr is not None else np.nan

            pv1 = _get(ind, ["confidence_v1", "confidence"])
            pv2 = _get(ind, ["confidence_v2"])
            cv1 = _get(ind, ["confidence_cal_v1"])
            cv2 = _get(ind, ["confidence_cal_v2"])

            if pv1 is None:
                continue
            y.append(yy); r.append(rr)
            p_v1.append(pv1)
            p_v2.append(pv2 if pv2 is not None else np.nan)
            c_v1.append(cv1 if cv1 is not None else np.nan)
            c_v2.append(cv2 if cv2 is not None else np.nan)

    return {
        "y": np.asarray(y, dtype=np.float64),
        "r": np.asarray(r, dtype=np.float64),
        "p_v1": np.asarray(p_v1, dtype=np.float64),
        "p_v2": np.asarray(p_v2, dtype=np.float64),
        "c_v1": np.asarray(c_v1, dtype=np.float64),
        "c_v2": np.asarray(c_v2, dtype=np.float64),
    }


def precision_topk(y: np.ndarray, p: np.ndarray, frac: float = 0.05) -> float:
    n = int(len(y))
    if n <= 0:
        return float("nan")
    k = max(1, int(n * frac))
    idx = np.argsort(-p)[:k]
    return float(y[idx].mean())


def expectancy_topk(r: np.ndarray, p: np.ndarray, frac: float = 0.05) -> float:
    n = int(len(p))
    if n <= 0:
        return float("nan")
    k = max(1, int(n * frac))
    idx = np.argsort(-p)[:k]
    rr = r[idx]
    rr = rr[np.isfinite(rr)]
    if len(rr) == 0:
        return float("nan")
    return float(rr.mean())


def report(y: np.ndarray, r: np.ndarray, p: np.ndarray, *, bins: int = 20, near_half_width: float = 0.05) -> Dict[str, float]:
    # Extended calibration report: ECE / MCE / Brier / slope / sharpness / prob_mass_near_half
    ext = extended_report(y, p, bins=bins, near_half_width=near_half_width)
    ext.update({
        "precision_top5pct": precision_topk(y, p, 0.05),
        "expectancy_r_top5pct": expectancy_topk(r, p, 0.05),
    })
    return ext


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_jsonl", required=True)
    ap.add_argument("--out_json", default="")
    ap.add_argument("--min_rows", type=int, default=500)
    ap.add_argument("--bins", type=int, default=20)
    ap.add_argument("--near_half_width", type=float, default=0.05)
    args = ap.parse_args()

    d = load(args.in_jsonl)
    y, r = d["y"], d["r"]
    if len(y) < args.min_rows:
        raise SystemExit(f"Not enough rows: {len(y)} < {args.min_rows}")

    out: Dict[str, Any] = {}
    out["raw_v1"] = report(y, r, d["p_v1"], bins=args.bins, near_half_width=args.near_half_width)

    m2 = np.isfinite(d["p_v2"])
    if int(m2.sum()) >= args.min_rows:
        out["raw_v2"] = report(y[m2], r[m2], d["p_v2"][m2], bins=args.bins, near_half_width=args.near_half_width)
    m1c = np.isfinite(d["c_v1"])
    if int(m1c.sum()) >= args.min_rows:
        out["cal_v1"] = report(y[m1c], r[m1c], d["c_v1"][m1c], bins=args.bins, near_half_width=args.near_half_width)
    m2c = np.isfinite(d["c_v2"])
    if int(m2c.sum()) >= args.min_rows:
        out["cal_v2"] = report(y[m2c], r[m2c], d["c_v2"][m2c], bins=args.bins, near_half_width=args.near_half_width)

    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2, sort_keys=True)
    print(json.dumps(out, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
