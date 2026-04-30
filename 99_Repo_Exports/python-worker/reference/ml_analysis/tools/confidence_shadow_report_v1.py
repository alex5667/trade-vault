"""Confidence Shadow Report (v1).

Reads the joined dataset JSONL produced by build_edge_stack_dataset_from_redis and evaluates:
  - Brier score
  - ECE
  - Precision@Top5%
  - Expectancy R @Top5%

Compares confidence_v1 vs confidence_v2 (shadow).

Usage:
  python -m ml_analysis.tools.confidence_shadow_report_v1 --in_jsonl ./edge_train.jsonl
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List, Tuple

import numpy as np

from ml_analysis.tools.edge_stack_shadow_metrics_p60 import calculate_shadow_metrics, check_promotion_guard


def _get_prob(row: Dict[str, Any], key: str) -> float | None:
    ind = row.get("indicators") or {}
    
    # Unified keys support
    # If key is "confidence_v1", try "confidence_cal_v1" (if cal exists) -> "confidence_cal" -> "confidence_v1" -> "confidence"
    # Actually report usually wants "Raw" vs "Cal".
    # The prompt says: "reads unified-keys with fallback to old".
    # Let's support looking up exactly what is asked, but if we ask for "confidence_correct", we might need logic.
    # But here we just look up keys. 
    # v1 raw key: "confidence_raw" or "confidence_v1"
    # v1 cal key: "confidence_cal" or "confidence_cal_v1"
    
    v = ind.get(key)
    if v is None:
        # Fallbacks
        if key == "confidence_v1": 
             v = ind.get("confidence_raw") or ind.get("confidence")
        elif key == "confidence_cal_v1":
             v = ind.get("confidence_cal")
        elif key == "confidence_v2":
             v = ind.get("confidence_raw_v2")

    if v is None:
        return None
    try:
        f = float(v)
        if not np.isfinite(f):
            return None
        # clamp
        if f < 0.0:
            return 0.0
        if f > 1.0:
            return 1.0
        return f
    except Exception:
        return None


def load_arrays_custom(path: str, prob_keys: List[str]) -> Tuple[np.ndarray, np.ndarray, np.ndarray, ...]:
    y: List[int] = []
    r: List[float] = []
    
    # Dynamic list of lists for probs
    probs_lists = [[] for _ in prob_keys]

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            yy = int(row.get("y", 0) or 0)
            rr = row.get("r_mult", None)
            rr = float(rr) if rr is not None else np.nan
            
            # Read all probs
            vals = []
            for k in prob_keys:
                vals.append(_get_prob(row, k))
            
            # Filter? Usually we filter if "primary" is missing.
            # Here we just iterate. If p1 is None, we skip line? 
            # Original code: required v1.
            # Let's require the first key in the list to be present?
            # Or just append Nan? 
            # Since we return aligned arrays, we must either skip or append Nan/default.
            # _get_prob returns None if missing or infinite.
            # Let's require the first key (Raw or Base) to be present for the row to count?
            if vals[0] is None:
                continue

            y.append(yy)
            r.append(rr)
            
            for i, v in enumerate(vals):
                probs_lists[i].append(v if v is not None else np.nan)

    res = [
        np.array(y, dtype=np.float32)
        np.array(r, dtype=np.float32)
    ]
    for pl in probs_lists:
        res.append(np.array(pl, dtype=np.float32))
        
    return tuple(res)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_jsonl", required=True, help="Joined dataset JSONL")
    ap.add_argument("--min_rows", type=int, default=200, help="Minimum rows required")
    args = ap.parse_args()


    # Load separate arrays for Raw/Cal if available? 
    # load_arrays currently returns p1, p2 (based on passed keys, but here hardcoded).
    # We should refactor load_arrays or just load more.
    # Let's update the main section to load 4 prob arrays: v1_raw, v1_cal, v2_raw, v2_cal.
    
    # Reload with specific keys
    y, r, p1_raw, p1_cal = load_arrays_custom(args.in_jsonl, ["confidence_v1", "confidence_cal_v1"])
    _, _, p2_raw, p2_cal = load_arrays_custom(args.in_jsonl, ["confidence_v2", "confidence_cal_v2"])
    
    n = int(len(y))
    if n < args.min_rows:
        raise SystemExit(f"Not enough rows for stable metrics: {n} < {args.min_rows}")

    print(f"--- Report (N={n}) ---")

    # V1 Raw
    m1_raw = calculate_shadow_metrics(y_true=y, y_prob=p1_raw, y_r=r)
    print("Base (v1 raw)", {k: round(float(v), 6) for k, v in m1_raw.items()})

    # V1 Cal (if available)
    if np.isfinite(p1_cal).sum() > 0:
        m1_cal = calculate_shadow_metrics(y_true=y, y_prob=p1_cal, y_r=r)
        print("Base (v1 cal)", {k: round(float(v), 6) for k, v in m1_cal.items()})
    else:
         print("Base (v1 cal) - N/A")

    # V2 Raw
    mask2 = np.isfinite(p2_raw)
    if mask2.sum() > args.min_rows:
        m2_raw = calculate_shadow_metrics(y_true=y[mask2], y_prob=p2_raw[mask2], y_r=r[mask2])
        print("V2 (raw)     ", {k: round(float(v), 6) for k, v in m2_raw.items()})
        
        # V2 Cal
        if np.isfinite(p2_cal[mask2]).sum() > args.min_rows:
             m2_cal = calculate_shadow_metrics(y_true=y[mask2], y_prob=p2_cal[mask2], y_r=r[mask2])
             print("V2 (cal)     ", {k: round(float(v), 6) for k, v in m2_cal.items()})
    else:
        print("V2 (raw)      - N/A")

    # promotion guard (default tolerances inside function)
    # Select Best (Calibrated if avail, else Raw) for Guard
    m1 = m1_cal if 'm1_cal' in locals() else m1_raw
    m2 = m2_cal if 'm2_cal' in locals() else (m2_raw if 'm2_raw' in locals() else None)

    if m2 is not None:
        ok, reasons = check_promotion_guard(champion_metrics=m1, candidate_metrics=m2)
        print("Promotion guard", {"ok": bool(ok), "reasons": reasons})
    else:
        print("Promotion guard skipped (no V2 metrics)")


if __name__ == "__main__":
    main()
