from __future__ import annotations
"""Calibration tool: подбирать w_exec_risk, exec_risk_ref_bps, of_score_min_* по целевой функции (meanR/tail).

Why:
  Offline calibration на dataset NDJSON (из build_of_dataset.py) для подбора оптимальных параметров
  по целевой функции (meanR_pass, tail_rate_pass) с ограничениями (pass_rate, tail_max).

Usage:
  python -m tools.calibrate_gate_params --dataset /tmp/dataset.ndjson --out /tmp/calib.json --w-exec-grid 0.14,0.18,0.22,0.26 --exec-ref-grid 8,10,12 --score-min-grid 0.62,0.65,0.68,0.70
"""


import argparse
import json
from typing import Any, Dict, List, Tuple


def iter_ndjson(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return int(d)


def eval_policy(rows: List[Dict[str, Any]], *, w_exec: float, exec_ref_bps: float, score_min: float) -> Dict[str, float]:
    """
    Offline approximation:
      score_adj = clamp01(base_score - w_exec * (exec_risk_bps/exec_ref_bps))
    We treat row["score"] as base_score (from engine replay score_final may already include penalty).
    For calibration, we need base_score; если у вас в evidence.score_breakdown есть base_score — используйте его.
    Здесь используем:
      base_score = evidence.score_breakdown.base_score if present else row.score
    Gate pass criteria:
      ok_cal = (base_score - penalty >= score_min) AND (ok==1)
    (ok==1 preserves hard vetoes from other gates)
    """
    n = 0
    n_pass = 0
    sum_r = 0.0
    sum_r_pass = 0.0
    tail = 0
    tail_pass = 0

    for r in rows:
        n += 1
        rm = _f(r.get("r_mult", 0.0))
        sum_r += rm
        if rm <= -1.0:
            tail += 1

        base_score = _f(r.get("score", 0.0))
        # if you stored base_score in dataset:
        if "base_score" in r:
            base_score = _f(r.get("base_score"))

        exec_bps = _f(r.get("exec_risk_bps", 0.0))
        penalty = 0.0
        if exec_ref_bps > 0:
            penalty = w_exec * max(0.0, exec_bps / exec_ref_bps)
        score_adj = max(0.0, min(1.0, base_score - penalty))

        ok_hard = _i(r.get("ok", 0))
        ok_cal = 1 if (ok_hard == 1 and score_adj >= score_min) else 0

        if ok_cal == 1:
            n_pass += 1
            sum_r_pass += rm
            if rm <= -1.0:
                tail_pass += 1

    meanR = sum_r / n if n else 0.0
    meanR_pass = sum_r_pass / n_pass if n_pass else 0.0
    tail_rate = tail / n if n else 0.0
    tail_rate_pass = tail_pass / n_pass if n_pass else 0.0
    pass_rate = n_pass / n if n else 0.0

    # objective: maximize meanR_pass while constraining tail_rate_pass
    return {
        "n": float(n),
        "pass_rate": float(pass_rate),
        "meanR_all": float(meanR),
        "meanR_pass": float(meanR_pass),
        "tail_pass": float(tail_rate_pass),
        "tail_all": float(tail_rate),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--w-exec-grid", default="0.14,0.18,0.22,0.26")
    ap.add_argument("--exec-ref-grid", default="8,10,12")
    ap.add_argument("--score-min-grid", default="0.62,0.65,0.68,0.70")
    ap.add_argument("--tail-max", type=float, default=0.18, help="max tail loss rate among passed trades")
    ap.add_argument("--pass-min", type=float, default=0.15, help="min pass rate to avoid overfitting too strict")
    args = ap.parse_args()

    rows = list(iter_ndjson(args.dataset))

    w_grid = [float(x) for x in args.w_exec_grid.split(",") if x.strip()]
    ref_grid = [float(x) for x in args.exec_ref_grid.split(",") if x.strip()]
    score_grid = [float(x) for x in args.score_min_grid.split(",") if x.strip()]

    best = None
    best_obj = -1e9

    for w in w_grid:
        for ref in ref_grid:
            for smin in score_grid:
                m = eval_policy(rows, w_exec=w, exec_ref_bps=ref, score_min=smin)
                if m["pass_rate"] < args.pass_min:
                    continue
                if m["tail_pass"] > args.tail_max:
                    continue
                # objective
                obj = m["meanR_pass"]
                if obj > best_obj:
                    best_obj = obj
                    best = {"w_exec_risk": w, "exec_risk_ref_bps": ref, "of_score_min": smin, "metrics": m}

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump({"best": best, "best_obj": best_obj}, f, ensure_ascii=False, indent=2)

    print("best", best)


if __name__ == "__main__":
    main()

