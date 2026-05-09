from __future__ import annotations

"""Calibration report for raw vs calibrated confidences.

Computes:
  - ECE
  - Brier
  - Precision@Top5%
  - Expectancy R @Top5%

Requires joined JSONL with:
  - y (0/1)
  - r_mult (float) optional for expectancy
  - indicators.confidence_v1 / confidence_v2 and/or confidence_cal_v1 / confidence_cal_v2
"""


import argparse
import json
from typing import Any

import numpy as np


def _get(ind: dict[str, Any], keys: list[str]) -> float | None:
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


def load(path: str) -> dict[str, np.ndarray]:
    y, r = [], []
    p_v1, p_v2, c_v1, c_v2 = [], [], [], []
    with open(path, encoding="utf-8") as f:
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


def ece(y: np.ndarray, p: np.ndarray, bins: int = 20) -> float:
    p = np.clip(p, 1e-6, 1.0 - 1e-6)
    edges = np.linspace(0.0, 1.0, bins + 1)
    out = 0.0
    for i in range(bins):
        lo, hi = edges[i], edges[i + 1]
        m = (p >= lo) & (p < hi) if i < bins - 1 else (p >= lo) & (p <= hi)
        if not np.any(m):
            continue
        acc = float(y[m].mean())
        conf = float(p[m].mean())
        out += float(m.mean()) * abs(acc - conf)
    return float(out)


def brier(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((p - y) ** 2))


def precision_topk(y: np.ndarray, p: np.ndarray, frac: float = 0.05) -> float:
    n = int(len(y))
    k = max(1, int(n * frac))
    idx = np.argsort(-p)[:k]
    return float(y[idx].mean())


def expectancy_topk(r: np.ndarray, p: np.ndarray, frac: float = 0.05) -> float:
    n = int(len(p))
    k = max(1, int(n * frac))
    idx = np.argsort(-p)[:k]
    rr = r[idx]
    rr = rr[np.isfinite(rr)]
    if len(rr) == 0:
        return float('nan')
    return float(rr.mean())


def report(y: np.ndarray, r: np.ndarray, p: np.ndarray) -> dict[str, float]:
    return {
        "ece": ece(y, p),
        "brier": brier(y, p),
        "precision_top5pct": precision_topk(y, p, 0.05),
        "expectancy_r_top5pct": expectancy_topk(r, p, 0.05),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_jsonl", required=True)
    ap.add_argument("--min_rows", type=int, default=500)
    args = ap.parse_args()

    d = load(args.in_jsonl)
    y, r = d["y"], d["r"]
    if len(y) < args.min_rows:
        raise SystemExit(f"Not enough rows: {len(y)} < {args.min_rows}")

    out = {}
    out["raw_v1"] = report(y, r, d["p_v1"])

    m2 = np.isfinite(d["p_v2"])
    if int(m2.sum()) >= args.min_rows:
        out["raw_v2"] = report(y[m2], r[m2], d["p_v2"][m2])

    m1c = np.isfinite(d["c_v1"])
    if int(m1c.sum()) >= args.min_rows:
        out["cal_v1"] = report(y[m1c], r[m1c], d["c_v1"][m1c])

    m2c = np.isfinite(d["c_v2"])
    if int(m2c.sum()) >= args.min_rows:
        out["cal_v2"] = report(y[m2c], r[m2c], d["c_v2"][m2c])

    print(json.dumps(out, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
