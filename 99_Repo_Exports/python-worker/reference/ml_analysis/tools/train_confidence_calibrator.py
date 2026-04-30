"""Train confidence calibrator (temperature / Platt) on joined JSONL.

Input JSONL: each line is a dict with at least:
  - y: 0/1 label
  - indicators.confidence (or confidence_v1)
Optional:
  - indicators.confidence_v2

Output JSON:
  {schema_version:1, type:'temp_logit'|'platt_logit', ...params...}

Why logit-domain?
  Both temperature scaling and Platt scaling operate on logits. Here we treat
  raw confidence as a probability proxy and calibrate its logit.
"""

from __future__ import annotations

import argparse
import json
import math
from typing import Any, Dict, List, Tuple

import numpy as np


def _clamp01(p: float, eps: float) -> float:
    return float(max(eps, min(1.0 - eps, p)))


def _logit(p: np.ndarray, eps: float) -> np.ndarray:
    p = np.clip(p, eps, 1.0 - eps)
    return np.log(p / (1.0 - p))


def _sigmoid(z: np.ndarray) -> np.ndarray:
    # stable sigmoid
    out = np.empty_like(z, dtype=np.float64)
    pos = z >= 0
    out[pos] = 1.0 / (1.0 + np.exp(-z[pos]))
    ez = np.exp(z[~pos])
    out[~pos] = ez / (1.0 + ez)
    return out


def load_y_p(path: str, key: str, eps: float) -> Tuple[np.ndarray, np.ndarray]:
    y: List[int] = []
    p: List[float] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            yy = int(row.get("y", 0) or 0)
            ind = row.get("indicators") or {}
            v = ind.get(key)
            if v is None and key == "confidence_v1":
                v = ind.get("confidence")
            if v is None:
                continue
            try:
                pv = float(v)
                if not math.isfinite(pv):
                    continue
                pv = _clamp01(pv, eps)
            except Exception:
                continue
            y.append(yy)
            p.append(pv)
    return np.asarray(y, dtype=np.float64), np.asarray(p, dtype=np.float64)


def fit_temp_on_logit(y: np.ndarray, p: np.ndarray, eps: float) -> float:
    """Fit temperature T by minimizing NLL for sigmoid(z/T) where z=logit(p)."""
    z = _logit(p, eps)

    # 1D search on logT (stable). Coarse-to-fine.
    def nll(logT: float) -> float:
        T = float(np.exp(logT))
        q = _sigmoid(z / T)
        q = np.clip(q, eps, 1.0 - eps)
        return float(-(y * np.log(q) + (1.0 - y) * np.log(1.0 - q)).mean())

    # coarse grid
    grid = np.linspace(np.log(0.25), np.log(4.0), 49)
    vals = np.array([nll(g) for g in grid], dtype=np.float64)
    i = int(vals.argmin())
    best = float(grid[i])

    # refine with small steps around best
    step = 0.05
    for _ in range(60):
        cands = np.array([best - step, best, best + step], dtype=np.float64)
        v = np.array([nll(c) for c in cands], dtype=np.float64)
        j = int(v.argmin())
        if j == 1:
            step *= 0.6
        else:
            best = float(cands[j])
        if step < 1e-4:
            break

    return float(np.exp(best))


def fit_platt_on_logit(y: np.ndarray, p: np.ndarray, eps: float) -> Tuple[float, float]:
    """Fit Platt parameters a,b on logit(p) using Newton steps (2 params)."""
    z = _logit(p, eps)
    a = 1.0
    b = 0.0
    for _ in range(60):
        s = a * z + b
        q = _sigmoid(s)
        # gradients
        g_a = np.mean((q - y) * z)
        g_b = np.mean(q - y)
        # Hessian
        w = q * (1.0 - q)
        h_aa = np.mean(w * z * z) + 1e-9
        h_ab = np.mean(w * z)
        h_bb = np.mean(w) + 1e-9
        # solve 2x2
        det = h_aa * h_bb - h_ab * h_ab
        if det <= 1e-12:
            break
        da = ( h_bb * g_a - h_ab * g_b) / det
        db = (-h_ab * g_a + h_aa * g_b) / det
        # damped update
        a -= float(da)
        b -= float(db)
        if abs(da) < 1e-6 and abs(db) < 1e-6:
            break
    return float(a), float(b)


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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_jsonl", required=True)
    ap.add_argument("--out_json", required=True)
    ap.add_argument("--key", default="confidence_v1", help="confidence_v1 or confidence_v2")
    ap.add_argument("--method", default="temp", choices=["temp", "platt"])
    ap.add_argument("--eps", type=float, default=1e-6)
    ap.add_argument("--min_rows", type=int, default=500)
    args = ap.parse_args()

    y, p = load_y_p(args.in_jsonl, key=args.key, eps=args.eps)
    if len(y) < args.min_rows:
        raise SystemExit(f"Not enough rows: {len(y)} < {args.min_rows}")

    if args.method == "temp":
        T = fit_temp_on_logit(y, p, eps=args.eps)
        z = _logit(p, args.eps)
        p_cal = _sigmoid(z / T)
        payload = {"schema_version": 1, "type": "temp_logit", "t": float(T), "eps": float(args.eps)}
    else:
        a, b0 = fit_platt_on_logit(y, p, eps=args.eps)
        z = _logit(p, args.eps)
        p_cal = _sigmoid(a * z + b0)
        payload = {"schema_version": 1, "type": "platt_logit", "a": float(a), "b": float(b0), "eps": float(args.eps)}

    report = {
        "rows": int(len(y))
        "raw": {"ece": ece(y, p), "brier": brier(y, p)}
        "cal": {"ece": ece(y, p_cal), "brier": brier(y, p_cal)}
    }
    payload["train_report"] = report

    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)

    print("Saved", args.out_json)
    print("Report", report)


if __name__ == "__main__":
    main()
