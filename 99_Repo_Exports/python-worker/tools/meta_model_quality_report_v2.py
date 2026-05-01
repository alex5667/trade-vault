# python-worker/tools/meta_model_quality_report_v2.py
from __future__ import annotations
"""
Regime/session aware quality report for MetaModelLR.

Outputs:
- global metrics
- per-group metrics (regime/session buckets etc.)
- "worst" summary (min PR-AUC / max ECE among sufficiently-sized groups)

Design notes:
- Train==Serve: features are read from (evidence) first, then (indicators).
- Works with nested parquet where 'indicators' column stores dict/struct.
- Avoids high-cardinality Prometheus labels: exports only global + worst-group summaries by default.
"""

import argparse
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import pandas as pd  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit("pandas is required for meta_model_quality_report_v2.py") from e

try:
    import numpy as np  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit("numpy is required for meta_model_quality_report_v2.py") from e

try:
    from sklearn.metrics import average_precision_score  # type: ignore
except Exception:
    average_precision_score = None  # type: ignore

try:
    from core.meta_model_lr import MetaModelLR  # type: ignore
except Exception:
    MetaModelLR = None  # type: ignore


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _as_dict(x: Any) -> Dict[str, Any]:
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    # pyarrow struct may come as dict-like; pandas may return mapping
    try:
        if hasattr(x, "items"):
            return dict(x.items())  # type: ignore
    except Exception:
        pass
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return {}
        # JSON string
        if s[0] in "{[":
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


def _get_nested(row: Dict[str, Any], key: str) -> Dict[str, Any]:
    # row can be dict (from df.iloc[i].to_dict())
    v = row.get(key)
    return _as_dict(v)


def _get_feat_value(name: str, evidence: Dict[str, Any], indicators: Dict[str, Any]) -> Any:
    if name in evidence:
        return evidence.get(name)
    # allow indicators may nest again under "indicators" (rare but seen in some pipelines)
    if name in indicators:
        return indicators.get(name)
    inner = indicators.get("indicators")
    if isinstance(inner, dict) and name in inner:
        return inner.get(name)
    return 0.0


def _build_feat_for_model(features: List[str], evidence: Dict[str, Any], indicators: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for f in features:
        out[f] = _get_feat_value(f, evidence, indicators)
    return out


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _predict_proba(model_json: Dict[str, Any], feat: Dict[str, Any]) -> float:
    # fallback if core.MetaModelLR is unavailable
    intercept = float(model_json.get("intercept", 0.0))
    coef = model_json.get("coef") or []
    features = model_json.get("features") or []
    s = intercept
    for name, w in zip(features, coef):
        v = _finite_float(feat.get(name, 0.0), 0.0)
        # NOTE: transforms/robust_scaler are not applied in fallback.
        s += float(w) * float(v)
    return float(_sigmoid(float(s)))


def _compute_brier(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((p - y) ** 2))


def _compute_ece(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    # uniform bins over probability [0,1]
    if len(y) == 0:
        return 0.0
    p_clip = np.clip(p, 0.0, 1.0)
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
    # stable: argsort descending with mergesort
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


def _write_prom_textfile(path: str, lines: List[str]) -> None:
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
    ap.add_argument("--topk", type=int, default=int(os.environ.get("META_REPORT_TOPK", "200")))
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--prom-textfile", default=os.environ.get("META_REPORT_PROM_TEXTFILE", ""))
    args = ap.parse_args()

    model_path = args.model_json
    model_raw = json.loads(open(model_path, "r", encoding="utf-8").read())
    features = list(model_raw.get("features") or [])
    schema_name = str(model_raw.get("schema_name") or model_raw.get("schema") or "")
    schema_version = model_raw.get("schema_version")
    schema_hash = str(model_raw.get("feature_cols_hash") or "")

    model = None
    if MetaModelLR is not None:
        try:
            model = MetaModelLR.load(model_path)
        except Exception:
            model = None

    group_cols = [c.strip() for c in str(args.group_cols).split(",") if c.strip()]

    # read parquet
    df = pd.read_parquet(args.dataset_parquet)

    # label
    if args.label_col not in df.columns:
        raise SystemExit(f"label_col='{args.label_col}' not found in parquet columns={list(df.columns)[:50]}")
    y = df[args.label_col].astype(float).to_numpy()

    # Build p by iterating rows (dict columns, nested)
    p = np.zeros(len(df), dtype=float)

    # prefetch dict columns if exist
    has_ev = args.evidence_col in df.columns
    has_ind = args.indicators_col in df.columns

    for i in range(len(df)):
        row = df.iloc[i].to_dict()
        evidence = _as_dict(row.get(args.evidence_col)) if has_ev else {}
        indicators = _as_dict(row.get(args.indicators_col)) if has_ind else {}
        feat = _build_feat_for_model(features, evidence, indicators)
        if model is not None:
            try:
                p[i] = float(model.predict_proba(feat))
            except Exception:
                p[i] = float(_predict_proba(model_raw, feat))
        else:
            p[i] = float(_predict_proba(model_raw, feat))

    global_pack = _calc_metrics(y, p, topk=args.topk, ece_bins=args.ece_bins)

    # Groups
    groups_out: Dict[str, Any] = {}
    group_metrics: List[Tuple[str, MetricPack]] = []

    if group_cols:
        # build group keys
        keys: List[str] = []
        for i in range(len(df)):
            row = df.iloc[i].to_dict()
            parts = []
            for c in group_cols:
                v = row.get(c)
                if v is None and has_ind:
                    ind = _as_dict(row.get(args.indicators_col))
                    v = ind.get(c)
                parts.append(f"{c}={v}")
            keys.append("|".join(parts))

        # aggregate indices per group
        buckets: Dict[str, List[int]] = {}
        for i, k in enumerate(keys):
            buckets.setdefault(k, []).append(i)

        for k, idxs in buckets.items():
            if len(idxs) < int(args.min_group_n):
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
                },
            }

    # worst summary (only among evaluated groups)
    worst = {
        "group_n_min": int(args.min_group_n),
        "coverage_groups": int(len(group_metrics)),
        "worst_pr_auc": None,
        "worst_pr_auc_group": None,
        "worst_precision_top5p": None,
        "worst_precision_top5p_group": None,
        "worst_ece": None,
        "worst_ece_group": None,
    }
    if group_metrics:
        # worst PR-AUC / precision (min), worst ECE (max)
        pr_min = min(group_metrics, key=lambda kv: kv[1].pr_auc)
        prec_min = min(group_metrics, key=lambda kv: kv[1].precision_top5p)
        ece_max = max(group_metrics, key=lambda kv: kv[1].ece)
        worst["worst_pr_auc"] = float(pr_min[1].pr_auc)
        worst["worst_pr_auc_group"] = pr_min[0]
        worst["worst_precision_top5p"] = float(prec_min[1].precision_top5p)
        worst["worst_precision_top5p_group"] = prec_min[0]
        worst["worst_ece"] = float(ece_max[1].ece)
        worst["worst_ece_group"] = ece_max[0]

        # stability
        eces = np.array([m.ece for _, m in group_metrics], dtype=float)
        prs = np.array([m.pr_auc for _, m in group_metrics], dtype=float)
        precs = np.array([m.precision_top5p for _, m in group_metrics], dtype=float)
        stability = {
            "ece_std": float(np.std(eces)),
            "pr_auc_std": float(np.std(prs)),
            "precision_top5p_std": float(np.std(precs)),
        }
    else:
        stability = {"ece_std": 0.0, "pr_auc_std": 0.0, "precision_top5p_std": 0.0}

    out = {
        "schema": {
            "name": schema_name,
            "version": schema_version,
            "feature_cols_hash": schema_hash,
        },
        "counts": {"n": global_pack.n, "pos": global_pack.pos, "neg": global_pack.neg},
        "metrics": {
            "brier": global_pack.brier,
            "ece": global_pack.ece,
            "pr_auc": global_pack.pr_auc,
            "precision_top5p": global_pack.precision_top5p,
            "precision_topk": global_pack.precision_topk,
        },
        "groups": groups_out,
        "worst": worst,
        "stability": stability,
        "params": {
            "group_cols": group_cols,
            "min_group_n": int(args.min_group_n),
            "topk": int(args.topk),
            "ece_bins": int(args.ece_bins),
            "label_col": args.label_col,
        },
        "generated_at": _now_iso(),
    }

    os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    if args.prom_textfile:
        # export only global + worst summaries (avoid group label explosion)
        sc = schema_name.replace('"', '\\"')
        lines: List[str] = []
        lines.append(f'meta_quality_ece{{schema="{sc}"}} {global_pack.ece}')
        lines.append(f'meta_quality_brier{{schema="{sc}"}} {global_pack.brier}')
        lines.append(f'meta_quality_pr_auc{{schema="{sc}"}} {global_pack.pr_auc}')
        lines.append(f'meta_quality_precision_top5p{{schema="{sc}"}} {global_pack.precision_top5p}')
        lines.append(f'meta_quality_precision_topk{{schema="{sc}"}} {global_pack.precision_topk}')
        lines.append(f'meta_quality_group_coverage{{schema="{sc}"}} {worst["coverage_groups"]}')
        if worst["worst_ece"] is not None:
            lines.append(f'meta_quality_worst_group_ece{{schema="{sc}"}} {worst["worst_ece"]}')
        if worst["worst_pr_auc"] is not None:
            lines.append(f'meta_quality_worst_group_pr_auc{{schema="{sc}"}} {worst["worst_pr_auc"]}')
        if worst["worst_precision_top5p"] is not None:
            lines.append(f'meta_quality_worst_group_precision_top5p{{schema="{sc}"}} {worst["worst_precision_top5p"]}')
        lines.append(f'meta_quality_group_pr_auc_std{{schema="{sc}"}} {stability["pr_auc_std"]}')
        lines.append(f'meta_quality_group_ece_std{{schema="{sc}"}} {stability["ece_std"]}')
        lines.append(f'meta_quality_group_precision_top5p_std{{schema="{sc}"}} {stability["precision_top5p_std"]}')
        _write_prom_textfile(args.prom_textfile, lines)


if __name__ == "__main__":
    main()
