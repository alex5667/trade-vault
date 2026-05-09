#!/usr/bin/env python3
from __future__ import annotations

"""
Meta Model Quality Report V3.

Improvements over V2:
- Feature extraction priority: root column -> evidence[key] -> indicators[key]
- Dynamic grouping: calculates session_bucket/dow_bucket from timestamp if missing
- Robust metrics: Brier, ECE, PR-AUC, Precision@TopK, Worst-Group analysis
"""

import argparse
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any

try:
    import pandas as pd  # type: ignore
except Exception as e:
    raise SystemExit("pandas is required for meta_model_quality_report_v3.py") from e

try:
    import numpy as np  # type: ignore
except Exception as e:
    raise SystemExit("numpy is required for meta_model_quality_report_v3.py") from e

try:
    from sklearn.metrics import average_precision_score  # type: ignore
except Exception:
    average_precision_score = None


try:
    from core.meta_model_lr import MetaModelLR
except Exception:
    MetaModelLR = None


def _parse_thresholds(s: str) -> list[float]:
    try:
        xs = [float(x.strip()) for x in str(s).split(',') if x.strip()]
        xs = [x for x in xs if not math.isnan(x) and not math.isinf(x)]
        xs.sort(reverse=True)
        return xs if xs else [0.9, 0.8, 0.7, 0.6, 0.5]
    except Exception:
        return [0.9, 0.8, 0.7, 0.6, 0.5]


def _dq_bucket(score: float | None, thresholds: list[float]) -> str:
    """Bucketize dq_health_score into low-cardinality labels."""
    if score is None:
        return "na"
    try:
        v = float(score)
        if math.isnan(v) or math.isinf(v):
            return "na"
    except Exception:
        return "na"
    # thresholds are sorted desc (good->bad).
    for i, thr in enumerate(thresholds):
        if v >= float(thr):
            return f"dq{i}"
    return f"dq{len(thresholds)}"


def _pearson_corr(x: np.ndarray, y: np.ndarray) -> float:
    if x.size < 2 or y.size < 2:
        return 0.0
    xv = x.astype(float) - float(np.mean(x))
    yv = y.astype(float) - float(np.mean(y))
    dx = float(np.sqrt(np.sum(xv * xv)))
    dy = float(np.sqrt(np.sum(yv * yv)))
    denom = dx * dy
    if denom <= 0.0:
        return 0.0
    return float(np.sum(xv * yv) / denom)


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _as_dict(x: Any) -> dict[str, Any]:
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    try:
        if hasattr(x, "items"):
            return dict(x.items())
    except Exception:
        pass
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return {}
        if s.startswith("{") or s.startswith("["):
            try:
                v = json.loads(s)
                return v if isinstance(v, dict) else {}
            except Exception:
                return {}
    return {}


def _finite_float(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        v = float(x)
        if not math.isfinite(v):
            return d
        return v
    except Exception:
        return d


def _get_feat_value(name: str, row: dict[str, Any], evidence: dict[str, Any], indicators: dict[str, Any]) -> Any:
    # 1. Root column
    if name in row:
        return row[name]
    # 2. Evidence
    if name in evidence:
        return evidence[name]
    # 3. Indicators
    if name in indicators:
        return indicators[name]
    # 4. Indicators nested (fallback)
    inner = indicators.get("indicators")
    if isinstance(inner, dict) and name in inner:
        return inner[name]
    return 0.0


def _build_feat_for_model(features: list[str], row: dict[str, Any], evidence: dict[str, Any], indicators: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for f in features:
        out[f] = _get_feat_value(f, row, evidence, indicators)
    return out


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _predict_proba(model_json: dict[str, Any], feat: dict[str, Any]) -> float:
    intercept = float(model_json.get("intercept", 0.0))
    coef = model_json.get("coef") or []
    features = model_json.get("features") or []
    s = intercept
    for name, w in zip(features, coef):
        v = _finite_float(feat.get(name, 0.0), 0.0)
        s += float(w) * float(v)
    return float(_sigmoid(float(s)))


def _compute_brier(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((p - y) ** 2))


def _compute_ece(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    if len(y) == 0:
        return 0.0
    p_clip = np.clip(p, 0.0, 1.0)
    # uniform bins
    idx = np.minimum((p_clip * bins).astype(int), bins - 1)
    ece = 0.0
    n = float(len(y))
    for b in range(bins):
        m = idx == b
        if not np.any(m):
            continue
        pb = float(np.mean(p_clip[m]))
        yb = float(np.mean(y[m]))
        w = float(np.sum(m)) / n
        ece += w * abs(pb - yb)
    return float(ece)


def _precision_at_k(y: np.ndarray, p: np.ndarray, k: int) -> float:
    n = int(len(y))
    if n == 0:
        return 0.0
    kk = min(int(k), n)
    if kk <= 0:
        return 0.0
    idx = np.argsort(-p, kind="mergesort")[:kk]
    return float(np.mean(y[idx]))


def _precision_top_pct(y: np.ndarray, p: np.ndarray, pct: float) -> float:
    n = int(len(y))
    if n == 0:
        return 0.0
    k = max(1, int(round(n * float(pct))))
    return _precision_at_k(y, p, k)


@dataclass
class MetricPack:
    n: int
    pos: int
    neg: int
    brier: float
    ece: float
    pr_auc: float
    precision_top5p: float
    precision_topk: float


def _calc_metrics(y: np.ndarray, p: np.ndarray, topk: int, ece_bins: int) -> MetricPack:
    y01 = y.astype(float)
    n = int(len(y01))
    pos = int(np.sum(y01))
    neg = int(n - pos)
    brier = _compute_brier(y01, p) if n else 0.0
    ece = _compute_ece(y01, p, bins=ece_bins) if n else 0.0

    if average_precision_score is not None and pos > 0 and neg > 0:
        pr_auc = float(average_precision_score(y01, p))
    else:
        # Fallback if sklearn missing or all same class
        pr_auc = 0.0

    precision_top5p = _precision_top_pct(y01, p, 0.05) if n else 0.0
    precision_topk = _precision_at_k(y01, p, topk) if n else 0.0

    return MetricPack(
        n=n,
        pos=pos,
        neg=neg,
        brier=float(brier),
        ece=float(ece),
        pr_auc=float(pr_auc),
        precision_top5p=float(precision_top5p),
        precision_topk=float(precision_topk),
    )


def _derive_group_value(name: str, row: dict[str, Any], evidence: dict[str, Any], indicators: dict[str, Any]) -> str:
    # 1. Look in root/evidence/indicators in order
    val = None
    if name in row:
        val = row[name]
    elif name in evidence:
        val = evidence[name]
    elif name in indicators:
        val = indicators[name]

    if val is not None:
        return str(val)

    # 2. Dynamic generation for known buckets
    # Try getting timestamp from known columns
    ts = None
    if "t_ts_ms" in row:
        ts = row["t_ts_ms"]
    elif "ts_ms" in row:
        ts = row["ts_ms"]

    if ts is not None:
        try:
            ts_ms = int(ts)
            # Dow bucket: 0=Mon, 6=Sun
            # epoch ms -> seconds -> struct_time
            st = time.gmtime(ts_ms / 1000.0)

            if name == "dow_bucket":
                return str(st.tm_wday)

            if name == "session_bucket":
                # Simple session logic: UTC hour
                # Asia: 0-8, London: 8-16, NY: 16-24 ??
                # Just use hour for now or simple mapping if defined standard exists
                # Standard trade session approximate:
                h = st.tm_hour
                if 0 <= h < 8:
                    return "asia"
                elif 8 <= h < 16:
                    return "london"
                else:
                    return "ny"
        except Exception:
            pass

    return "na"


def _write_prom_textfile(path: str, lines: list[str]) -> None:
    tmp = f"{path}.tmp"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        for ln in lines:
            f.write(ln.rstrip() + "\n")
    os.replace(tmp, path)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-json", required=True)
    ap.add_argument("--dataset-parquet", required=True)
    ap.add_argument("--label-col", default=os.environ.get("META_LABEL_COL", "y"))
    ap.add_argument("--evidence-col", default=os.environ.get("META_EVIDENCE_COL", "evidence"))
    ap.add_argument("--indicators-col", default=os.environ.get("META_INDICATORS_COL", "indicators"))
    ap.add_argument("--group-cols", default=os.environ.get("META_REPORT_GROUP_COLS", "regime_bucket,session_bucket"))
    ap.add_argument("--min-group-n", type=int, default=int(os.environ.get("META_REPORT_MIN_GROUP_N", "200")))
    ap.add_argument("--ece-bins", type=int, default=int(os.environ.get("META_REPORT_ECE_BINS", "10")))
    ap.add_argument("--topk", type=int, default=int(os.getenv("META_REPORT_TOPK", "200")))
    ap.add_argument("--dq-health-key", default=os.getenv("META_REPORT_DQ_HEALTH_KEY", "dq_health_score"))
    ap.add_argument("--dq-health-fallback-key", default=os.getenv("META_REPORT_DQ_HEALTH_FALLBACK_KEY", "data_health"))
    ap.add_argument("--dq-health-bucket-col", default=os.getenv("META_REPORT_DQ_HEALTH_BUCKET_COL", "dq_health_bucket"))
    ap.add_argument("--dq-health-thresholds", default=os.getenv("META_REPORT_DQ_HEALTH_THRESHOLDS", "0.9,0.8,0.7,0.6,0.5"))
    ap.add_argument("--min-dq-bucket-n", type=int, default=int(os.getenv("META_REPORT_MIN_DQ_BUCKET_N", "200")))
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--prom-textfile", default=os.environ.get("META_REPORT_PROM_TEXTFILE", ""))
    args = ap.parse_args()

    # Load Model
    model_path = args.model_json
    with open(model_path, encoding="utf-8") as f:
        model_raw = json.load(f)

    features = list(model_raw.get("features") or [])
    schema_name = str(model_raw.get("schema_name") or model_raw.get("schema") or "")

    model = None
    if MetaModelLR is not None:
        try:
            model = MetaModelLR.load(model_path)
        except Exception:
            model = None

    # Load Data
    try:
        df = pd.read_parquet(args.dataset_parquet)
    except Exception as e:
        print(f"Error reading parquet {args.dataset_parquet}: {e}")
        return

    if args.label_col not in df.columns:
        print(f"Label col {args.label_col} not found in {list(df.columns)}")
        # If label missing, maybe can't compute quality? Or treat as 0?
        # Usually fatal for quality report.
        return

    y = df[args.label_col].fillna(0).astype(int).to_numpy()

    # Predict
    p = np.zeros(len(df), dtype=float)

    # Pre-check columns
    has_ev = args.evidence_col in df.columns
    has_ind = args.indicators_col in df.columns

    # Feature extraction & Prediction loop
    # Optimized: convert relevant cols to dicts once if possible, but row iteration is safest for nested json

    for i in range(len(df)):
        row = df.iloc[i].to_dict()

        # Parse json/structs
        evidence = _as_dict(row.get(args.evidence_col)) if has_ev else {}
        indicators = _as_dict(row.get(args.indicators_col)) if has_ind else {}

        feat = _build_feat_for_model(features, row, evidence, indicators)

        if model:
            try:
                p[i] = float(model.predict_proba(feat))
            except Exception:
                 p[i] = float(_predict_proba(model_raw, feat))
        else:
            p[i] = float(_predict_proba(model_raw, feat))

    # Global Metrics
    global_pack = _calc_metrics(y, p, topk=args.topk, ece_bins=args.ece_bins)

    n_total = len(df)
    y_arr = y
    p_arr = p
    p_list: list[float] = []
    miss_sum = 0

    dq_scores: list[float] = []
    dq_present_n = 0
    dq_missing_n = 0
    dq_thresholds = _parse_thresholds(args.dq_health_thresholds)

    group_cols = [c.strip() for c in str(args.group_cols).split(",") if c.strip()]
    group_vals: dict[str, list[str]] = {c: [] for c in group_cols}
    dq_buckets: list[str] = []

    # Re-iterate for DQ and group values
    for i in range(n_total):
        row = df.iloc[i].to_dict()
        evidence = _as_dict(row.get(args.evidence_col)) if has_ev else {}
        indicators = _as_dict(row.get(args.indicators_col)) if has_ind else {}

        # DQ Health Score
        dq_score = _get_feat_value(args.dq_health_key, row, evidence, indicators)
        if dq_score == 0.0: # Default 0.0 means not found, try fallback
            dq_score = _get_feat_value(args.dq_health_fallback_key, row, evidence, indicators)

        if dq_score is not None and dq_score != 0.0: # 0.0 is default for _get_feat_value if not found
            dq_scores.append(float(dq_score))
            dq_buckets.append(_dq_bucket(float(dq_score), dq_thresholds))
            dq_present_n += 1
        else:
            dq_scores.append(np.nan)
            dq_buckets.append("na")
            dq_missing_n += 1

        # Group values
        for c in group_cols:
            if c == "dow_bucket":
                group_vals[c].append(_derive_group_value("dow_bucket", row, evidence, indicators))
                continue
            if c == args.dq_health_bucket_col or c == "dq_health_bucket":
                # use dq bucket derived above
                group_vals[c].append(dq_buckets[-1] if dq_buckets else "na")
                continue
            v = _derive_group_value(c, row, evidence, indicators)
            group_vals[c].append(v)

    # Global Metrics
    global_metrics = {
        "n": int(n_total),
        "pos": int(np.sum(y_arr)),
        "neg": int(n_total - np.sum(y_arr)),
        "brier": _calc_metrics(y_arr.astype(int), p_arr, int(args.topk), int(args.ece_bins)).brier,
        "ece": _calc_metrics(y_arr.astype(int), p_arr, int(args.topk), int(args.ece_bins)).ece,
        "pr_auc": _calc_metrics(y_arr.astype(int), p_arr, int(args.topk), int(args.ece_bins)).pr_auc,
        "precision_top5p": _calc_metrics(y_arr.astype(int), p_arr, int(args.topk), int(args.ece_bins)).precision_top5p,
        "precision_topk": _calc_metrics(y_arr.astype(int), p_arr, int(args.topk), int(args.ece_bins)).precision_topk,
    }
    dq_arr = np.asarray(dq_scores, dtype=float)
    dq_mask = np.isfinite(dq_arr)
    dq_present = int(np.sum(dq_mask)) if dq_mask.size else 0
    dq_mean = float(np.mean(dq_arr[dq_mask])) if dq_present else 0.0
    dq_corr = _pearson_corr(p_arr[dq_mask], dq_arr[dq_mask]) if dq_present >= 2 else 0.0

    global_metrics["dq_health_mean"] = float(dq_mean)
    global_metrics["corr_meta_p_dq_health"] = float(dq_corr)

    # Groups
    groups_out: dict[str, Any] = {}
    group_metrics: list[tuple[str, MetricPack]] = []

    if group_cols:
        # Build grouping keys
        group_keys = []
        for i in range(n_total):
            parts = []
            for gc in group_cols:
                parts.append(f"{gc}={group_vals[gc][i]}")
            group_keys.append("|".join(parts))

        # Bucketing
        buckets: dict[str, list[int]] = {}
        for i, k in enumerate(group_keys):
            buckets.setdefault(k, []).append(i)

        # Compute group metrics
        for k, idxs in buckets.items():
            if len(idxs) < args.min_group_n:
                continue

            yy = y[idxs]
            pp = p[idxs]
            mp = _calc_metrics(yy, pp, topk=args.topk, ece_bins=args.ece_bins)
            group_metrics.append((k, mp))

            groups_out[k] = {
                "counts": {"n": mp.n, "pos": mp.pos, "neg": mp.neg},
                "metrics": {
                    "brier": mp.brier,
                    "ece": mp.ece,
                    "pr_auc": mp.pr_auc,
                    "precision_top5p": mp.precision_top5p,
                    "precision_topk": mp.precision_topk,
                }
            }

    # Worst Group Logic
    worst_metrics = {
        "group_n_min": int(args.min_group_n),
        "coverage_groups": int(len(group_metrics)),
        "worst_pr_auc": None,
        "worst_pr_auc_group": None,
        "worst_precision_top5p": None,
        "worst_precision_top5p_group": None,
        "worst_ece": None,
        "worst_ece_group": None,
    }

    stability = {
        "ece_std": 0.0,
        "pr_auc_std": 0.0,
        "precision_top5p_std": 0.0,
    }

    if group_metrics:
        pr_min = min(group_metrics, key=lambda x: x[1].pr_auc)
        prec_min = min(group_metrics, key=lambda x: x[1].precision_top5p)
        ece_max = max(group_metrics, key=lambda x: x[1].ece)

        worst_metrics["worst_pr_auc"] = float(pr_min[1].pr_auc)
        worst_metrics["worst_pr_auc_group"] = pr_min[0]
        worst_metrics["worst_precision_top5p"] = float(prec_min[1].precision_top5p)
        worst_metrics["worst_precision_top5p_group"] = prec_min[0]
        worst_metrics["worst_ece"] = float(ece_max[1].ece)
        worst_metrics["worst_ece_group"] = ece_max[0]

        eces = np.array([m.ece for _, m in group_metrics])
        prs = np.array([m.pr_auc for _, m in group_metrics])
        precs = np.array([m.precision_top5p for _, m in group_metrics])

        stability["ece_std"] = float(np.std(eces))
        stability["pr_auc_std"] = float(np.std(prs))
        stability["precision_top5p_std"] = float(np.std(precs))

    # DQ-bucket only worst (independent of group_cols)
    worst_dq_bucket = {"bucket": "", "n": 0, "pr_auc": 0.0, "ece": 0.0, "brier": 0.0, "precision_top5p": 0.0}
    if dq_present >= 1:
        dq_idx_map: dict[str, list[int]] = {}
        for i in range(n_total):
            b = dq_buckets[i] if i < len(dq_buckets) else "na"
            dq_idx_map.setdefault(b, []).append(i)
        worst_b: tuple[str, MetricPack] | None = None
        for b, idxs in dq_idx_map.items():
            if b == "na":
                continue
            if len(idxs) < int(args.min_dq_bucket_n):
                continue
            idx = np.asarray(idxs, dtype=int)
            yy = y_arr[idx]
            pp = p_arr[idx]
            st = _calc_metrics(yy.astype(int), pp, int(args.topk), int(args.ece_bins))
            if worst_b is None:
                worst_b = (b, st)
            else:
                if (st.pr_auc < worst_b[1].pr_auc) or (st.pr_auc == worst_b[1].pr_auc and st.ece > worst_b[1].ece):
                    worst_b = (b, st)
        if worst_b is not None:
            worst_dq_bucket = {
                "bucket": worst_b[0],
                "n": int(worst_b[1].n),
                "pr_auc": float(worst_b[1].pr_auc),
                "ece": float(worst_b[1].ece),
                "brier": float(worst_b[1].brier),
                "precision_top5p": float(worst_b[1].precision_top5p),
            }

    # Compile Final Report
    report = {
        "schema": {
            "name": schema_name,
            "version": model_raw.get("schema_version"),
            "hash": model_raw.get("feature_cols_hash"),
            "groups_covered": int(len(groups_out)),
            "missing_total": int(miss_sum),
            "missing_per_row_mean": float(miss_sum) / float(n_total) if n_total else 0.0,
            "dq_health_key": str(args.dq_health_key),
            "dq_health_fallback_key": str(args.dq_health_fallback_key),
            "dq_present_n": int(dq_present_n),
            "dq_missing_n": int(dq_missing_n),
        },
        "counts": {"n": global_pack.n, "pos": global_pack.pos, "neg": global_pack.neg},
        "metrics": {"global": global_metrics, "worst": worst_metrics, "worst_dq_bucket": worst_dq_bucket},
        "groups": groups_out,
        "worst": worst_metrics, # Keep for backward compatibility
        "stability": stability,
        "params": {
            "group_cols": group_cols,
            "min_group_n": args.min_group_n,
            "topk": args.topk,
            "ece_bins": args.ece_bins,
        },
        "generated_at": _now_iso(),
    }

    # Write Output
    if args.out_json:
        # ensure dir
        out_path = os.path.abspath(args.out_json)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
    else:
        print(json.dumps(report, indent=2))

    if args.prom_textfile:
        sc = schema_name.replace('"', '\\"')
        lines = [
            f'meta_quality_ece{{schema="{sc}"}} {global_pack.ece}',
            f'meta_quality_brier{{schema="{sc}"}} {global_pack.brier}',
            f'meta_quality_pr_auc{{schema="{sc}"}} {global_pack.pr_auc}',
            f'meta_quality_precision_top5p{{schema="{sc}"}} {global_pack.precision_top5p}',
            f'meta_quality_precision_topk{{schema="{sc}"}} {global_pack.precision_topk}',
            f'meta_quality_group_coverage{{schema="{sc}"}} {worst_metrics["coverage_groups"]}',
            f'meta_quality_worst_group_ece{{schema="{sc}"}} {worst_metrics["worst_ece"] or 0.0}',
            f'meta_quality_worst_group_pr_auc{{schema="{sc}"}} {worst_metrics["worst_pr_auc"] or 0.0}',
            f'meta_quality_corr_meta_p_dq_health{{schema="{sc}"}} {dq_corr}',
            f'meta_quality_dq_health_mean{{schema="{sc}"}} {dq_mean}',
            f'meta_quality_worst_dq_bucket_pr_auc{{schema="{sc}"}} {worst_dq_bucket["pr_auc"]}',
            f'meta_quality_worst_dq_bucket_ece{{schema="{sc}"}} {worst_dq_bucket["ece"]}',
            f'meta_quality_dq_present_n{{schema="{sc}"}} {dq_present_n}',
        ]

        # Stability
        lines.append(f'meta_quality_group_ece_std{{schema="{sc}"}} {stability["ece_std"]}')
        lines.append(f'meta_quality_group_pr_auc_std{{schema="{sc}"}} {stability["pr_auc_std"]}')
        lines.append(f'meta_quality_group_precision_top5p_std{{schema="{sc}"}} {stability["precision_top5p_std"]}')

        _write_prom_textfile(args.prom_textfile, lines)

if __name__ == "__main__":
    main()
