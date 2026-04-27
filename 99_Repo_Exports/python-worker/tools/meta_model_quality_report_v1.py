#!/usr/bin/env python3
"""Offline quality report for MetaModelLR (meta-labeling).

Computes:
  - Brier score
  - ECE (Expected Calibration Error)
  - PR-AUC (Average Precision)
  - Precision@TopK / TopPct
  - Optional expectancy by R if a column is provided
  - Optional breakdown by bucket columns (session/regime/etc.)

Designed for:
  - nightly exporter that writes JSON + optional Prometheus textfile metrics
"""

from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from core.meta_model_lr import MetaModelLR
from core.meta_features_v1 import META_FEAT_V1_COLS, META_FEAT_V1_NAME, build_meta_features_v1
from core.meta_features_v2 import META_FEAT_V2_NAME, build_meta_features_v2
from core.meta_features_v3 import META_FEAT_V3_NAME, build_meta_features_v3
from core.meta_features_v4 import META_FEAT_V4_NAME, build_meta_features_v4


@dataclass
class CalibBin:
    lo: float
    hi: float
    n: int
    avg_p: float
    avg_y: float


def _is_finite(x: float) -> bool:
    return math.isfinite(float(x))


def brier_score(p: np.ndarray, y: np.ndarray) -> float:
    return float(np.mean((p - y) ** 2))


def ece_score(p: np.ndarray, y: np.ndarray, n_bins: int = 10) -> Tuple[float, List[CalibBin]]:
    p = np.clip(p, 0.0, 1.0)
    bins: List[CalibBin] = []
    ece = 0.0
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    n = len(p)
    for i in range(n_bins):
        lo, hi = float(edges[i]), float(edges[i + 1])
        mask = (p >= lo) & (p < hi) if i < n_bins - 1 else (p >= lo) & (p <= hi)
        idx = np.where(mask)[0]
        if len(idx) == 0:
            bins.append(CalibBin(lo=lo, hi=hi, n=0, avg_p=0.0, avg_y=0.0))
            continue
        avg_p = float(np.mean(p[idx]))
        avg_y = float(np.mean(y[idx]))
        frac = float(len(idx) / max(1, n))
        ece += frac * abs(avg_p - avg_y)
        bins.append(CalibBin(lo=lo, hi=hi, n=int(len(idx)), avg_p=avg_p, avg_y=avg_y))
    return float(ece), bins


def average_precision(p: np.ndarray, y: np.ndarray) -> float:
    # sklearn-free AP implementation (stable enough for nightly)
    # Sort by p desc
    order = np.argsort(-p)
    y_sorted = y[order]
    tp = 0
    fp = 0
    precisions = []
    recalls = []
    total_pos = int(np.sum(y_sorted))
    if total_pos == 0:
        return 0.0
    for yi in y_sorted:
        if yi == 1:
            tp += 1
        else:
            fp += 1
        prec = tp / max(1, (tp + fp))
        rec = tp / total_pos
        precisions.append(prec)
        recalls.append(rec)
    # AP as area under precision-recall curve using step-wise integration
    ap = 0.0
    prev_rec = 0.0
    for prec, rec in zip(precisions, recalls):
        if rec > prev_rec:
            ap += prec * (rec - prev_rec)
            prev_rec = rec
    return float(ap)


def precision_at_topk(p: np.ndarray, y: np.ndarray, k: int) -> float:
    k = int(max(1, min(int(k), len(p))))
    order = np.argsort(-p)[:k]
    return float(np.mean(y[order]))


def precision_at_toppct(p: np.ndarray, y: np.ndarray, pct: float) -> float:
    pct = float(max(0.0, min(1.0, pct)))
    k = max(1, int(round(len(p) * pct)))
    return precision_at_topk(p, y, k)


def build_features_from_rows(rows: List[Dict[str, Any]], schema_name: str) -> List[Dict[str, float]]:
    feats = []
    
    # Select builder
    # Dispatch builder based on schema_name (which comes from model usually, or passed in)
    if schema_name == META_FEAT_V3_NAME:
        builder = build_meta_features_v3
    elif schema_name == META_FEAT_V2_NAME:
        builder = build_meta_features_v2
    elif schema_name == META_FEAT_V4_NAME:
        builder = build_meta_features_v4
    else:
        builder = build_meta_features_v1

    for r in rows:
        # Map typical dataset fields -> builder inputs. This expects parquet rows already contain
        # evidence/indicator fields (as in your dataset generation).
        have = int(r.get("have", 0) or 0)
        need = int(r.get("need", 0) or 0)
        ok_soft = int(r.get("ok_soft", 0) or 0)
        rule_score = float(r.get("score_final_01", r.get("rule_score", 0.0)) or 0.0)
        exec_risk_norm = float(r.get("exec_risk_norm", 0.0) or 0.0)
        exec_risk_bps = float(r.get("exec_risk_bps", 0.0) or 0.0)
        ml_scenario = str(r.get("scenario_v4", r.get("ml_scenario", "")) or "")
        
        # Args logic: v1/v2/v3 signatures are compatible if we pass kwargs that they all accept
        # or we branch. Since they are all based on v1 signature roughly, but v2/v3 might need extras?
        # v2/v3 take 'evidence' which is 'row' here.
        
        # V3/V2 Builders have same signature for these basic args.
        if builder == build_meta_features_v3:
            feat, _ = builder(
                evidence=r,
                indicators=r,
                runtime_snap=None,
                runtime_prev_snap=None,
                indicators_with_v4=r,
                legs=r,
                have=have,
                need=need,
                ok_soft=ok_soft,
                rule_score=rule_score,
                exec_risk_norm=exec_risk_norm,
                exec_risk_bps=exec_risk_bps,
                ml_scenario=ml_scenario,
            )
        elif builder == build_meta_features_v4:
             feat, _ = builder(
                evidence=r,
                indicators=r,
                runtime_snap=None,
                runtime_prev_snap=None,
                indicators_with_v4=r,
                legs=r,
                have=have,
                need=need,
                ok_soft=ok_soft,
                rule_score=rule_score,
                exec_risk_norm=exec_risk_norm,
                exec_risk_bps=exec_risk_bps,
                ml_scenario=ml_scenario,
            )
        elif builder == build_meta_features_v2:
             feat, _ = builder(
                evidence=r,
                indicators=r,
                runtime_snap=None,
                runtime_prev_snap=None,
                indicators_with_v4=r,
                legs=r,
                have=have,
                need=need,
                ok_soft=ok_soft,
                rule_score=rule_score,
                exec_risk_norm=exec_risk_norm,
                exec_risk_bps=exec_risk_bps,
                ml_scenario=ml_scenario,
            )
        else:
            feat, _ = builder(
                evidence=r,
                indicators=r,
                indicators_with_v4=r,
                legs=r,
                have=have,
                need=need,
                ok_soft=ok_soft,
                rule_score=rule_score,
                exec_risk_norm=exec_risk_norm,
                exec_risk_bps=exec_risk_bps,
                ml_scenario=ml_scenario,
            )
            
        feats.append(feat)
    return feats


def write_prom_textfile(path: Path, metrics: Dict[str, float], labels: Optional[Dict[str, str]] = None) -> None:
    labels = labels or {}
    def fmt_labels(d: Dict[str, str]) -> str:
        if not d:
            return ""
        parts = [f'{k}="{str(v).replace("\\", "\\\\").replace("\"", "\\\"")}"' for k, v in d.items()]
        return "{" + ",".join(parts) + "}"

    lines = []
    for k, v in metrics.items():
        if not _is_finite(v):
            continue
        lines.append(f"{k}{fmt_labels(labels)} {float(v)}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.replace(str(tmp), str(path))


def compute_report(df: pd.DataFrame, model: MetaModelLR, label_col: str, r_col: str, group_cols: List[str]) -> Dict[str, Any]:
    y = df[label_col].astype(int).to_numpy()
    rows = df.to_dict(orient="records")
    
    schema_name = getattr(model, "schema_name", META_FEAT_V1_NAME)
    feats = build_features_from_rows(rows, schema_name)
    p = np.array([model.predict_proba(f) for f in feats], dtype=float)

    # Core metrics
    n = int(len(y))
    pos = int(np.sum(y))
    neg = n - pos
    brier = brier_score(p, y)
    ece, bins = ece_score(p, y, n_bins=10)
    pr_auc = average_precision(p, y)

    rep: Dict[str, Any] = {}
    
    # NEW (P4b): Nested 'metrics' and 'counts' for downstream parsers (ramp, dashboard)
    rep["counts"] = {
        "n": n,
        "pos": pos,
        "neg": neg,
    }
    rep["metrics"] = {
        "brier": brier,
        "ece": ece,
        "pr_auc": pr_auc,
    }

    # OLD (V1): Flat keys for backward compat
    rep["n"] = n
    rep["pos"] = pos
    rep["brier"] = brier
    rep["ece"] = ece
    rep["pr_auc"] = pr_auc

    rep["ece_bins"] = [asdict(b) for b in bins]
    
    # TopK (fixed)
    for k in [50, 100, 200, 500, 1000]:
        if len(y) >= k:
            val = precision_at_topk(p, y, k)
            rep[f"precision_at_{k}"] = val
            rep["metrics"][f"precision_at_{k}"] = val

    # TopPct
    for pct in [0.01, 0.02, 0.05, 0.1]:
        val = precision_at_toppct(p, y, pct)
        key = f"precision_top_{int(pct*100)}pct"
        rep[key] = val
        rep["metrics"][key] = val

    # Optional expectancy
    if r_col and r_col in df.columns:
        r = df[r_col].astype(float).to_numpy()
        exp_r = float(np.mean(r))
        rep["expectancy_r"] = exp_r
        rep["metrics"]["expectancy_r"] = exp_r
        
        # expectancy on top buckets (same pct list)
        order = np.argsort(-p)
        # Re-compute TopPct for expectancy_r
        for pct in [0.01, 0.02, 0.05, 0.1]:
            k = max(1, int(round(len(r) * pct)))
            val_r = float(np.mean(r[order[:k]]))
            key_r = f"expectancy_r_top_{int(pct*100)}pct"
            rep[key_r] = val_r
            rep["metrics"][key_r] = val_r

    # Group breakdown
    if group_cols:
        rep["groups"] = {}
        for gc in group_cols:
            if gc not in df.columns:
                continue
            rep["groups"][gc] = {}
            for gval, sub in df.groupby(gc):
                if len(sub) < 50:
                    continue
                subrep = compute_report(sub.copy(), model, label_col, r_col, group_cols=[])
                rep["groups"][gc][str(gval)] = subrep
    return rep


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-parquet", required=True)
    ap.add_argument("--model-json", required=True)
    ap.add_argument("--label-col", required=True)
    ap.add_argument("--r-col", default="", help="Optional column with trade return in R")
    ap.add_argument("--group-cols", default="", help="Comma-separated bucket cols, e.g. session_bucket,regime_bucket")
    ap.add_argument("--out-json", default="", help="Write report JSON (default: stdout)")
    ap.add_argument("--prom-textfile", default="", help="Optional Prometheus textfile output path")
    args = ap.parse_args()

    df = pd.read_parquet(args.in_parquet)
    model = MetaModelLR.load(args.model_json)

    group_cols = [x.strip() for x in str(args.group_cols or "").split(",") if x.strip()]
    report = compute_report(df, model, str(args.label_col), str(args.r_col or ""), group_cols)

    if args.out_json:
        Path(args.out_json).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))

    if args.prom_textfile:
        # keep cardinality low: only global metrics
        metrics = {
            "meta_quality_brier": float(report.get("brier", 0.0)),
            "meta_quality_ece": float(report.get("ece", 0.0)),
            "meta_quality_pr_auc": float(report.get("pr_auc", 0.0)),
        }
        # add a couple of topK metrics if exist
        for k in [100, 200, 500]:
            kk = f"precision_at_{k}"
            if kk in report:
                metrics[f"meta_quality_{kk}"] = float(report[kk])
        
        schema_name = getattr(model, "schema_name", META_FEAT_V1_NAME)
        write_prom_textfile(Path(args.prom_textfile), metrics, labels={"schema": str(schema_name)})

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
