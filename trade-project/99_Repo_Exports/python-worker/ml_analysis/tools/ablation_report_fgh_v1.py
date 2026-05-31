#!/usr/bin/env python3

from __future__ import annotations

"""Offline ablation report for derived ROI features F/G/H.

Goal:
  Compare baseline feature set vs +F / +G / +H / +FGH variants WITHOUT touching runtime.

Inputs:
  --data_jsonl: dataset built by ml_analysis/tools/build_edge_stack_dataset_from_redis.py
  --feature_cols_json: baseline feature cols (from --emit_feature_cols_json)

Output:
  Prints a per-regime (bucket) table with deltas vs baseline.
  Optionally writes JSON report (--out_json).
"""


import argparse
import json
import math
from typing import Any


def _topk_precision(y: list[int], p: list[float], frac: float) -> float:
    if not y:
        return float("nan")
    n = max(1, int(round(len(y) * float(frac))))
    idx = sorted(range(len(y)), key=lambda i: float(p[i]), reverse=True)[:n]
    if not idx:
        return float("nan")
    return float(sum(int(y[i]) for i in idx)) / float(len(idx))


def _safe_auc_roc(y: list[int], p: list[float]) -> float:
    # Minimal AUC implementation to avoid hard dependency on sklearn metrics.
    # If labels are all the same, return NaN.
    n_pos = sum(1 for v in y if int(v) == 1)
    n_neg = len(y) - n_pos
    if n_pos == 0 or n_neg == 0:
        return float("nan")
    # Rank-based (Mann–Whitney U)
    pairs = sorted(zip(p, y), key=lambda x: float(x[0]))
    rank_sum_pos = 0.0
    for i, (_pi, yi) in enumerate(pairs, start=1):
        if int(yi) == 1:
            rank_sum_pos += float(i)
    u = rank_sum_pos - (n_pos * (n_pos + 1) / 2.0)
    return float(u / (n_pos * n_neg))


def _fmt(x: Any) -> str:
    try:
        v = float(x)
        if not math.isfinite(v):
            return "nan"
        return f"{v:.4f}"
    except Exception:
        return "nan"


FGH_NUMERIC_KEYS: list[str] = [
    "rel_ofi_ml_norm_btc",
    "rel_lob_micro_shift_bps_btc",
    "ask_replenish_imb",
    "bid_replenish_imb",
    "lob_replenishment_pressure",
    "replenish_ratio_ask",
    "replenish_ratio_bid",
    "replenish_ratio_diff",
    "ofi_ml_wsum_vel",
    "micro_shift_bps_vel",
    "ofi_ml_wsum_vel_z_ema",
    "micro_shift_bps_vel_z_ema",
]


def _variant_feature_cols(base_cols: list[str]) -> dict[str, list[str]]:
    base = list(base_cols)
    add_f = ["f_rel_ofi_ml_norm_btc", "f_rel_lob_micro_shift_bps_btc"]
    add_g = [
        "f_ask_replenish_imb",
        "f_bid_replenish_imb",
        "f_lob_replenishment_pressure",
        "f_replenish_ratio_ask",
        "f_replenish_ratio_bid",
        "f_replenish_ratio_diff",
    ]
    add_h = [
        "f_ofi_ml_wsum_vel",
        "f_micro_shift_bps_vel",
        "f_ofi_ml_wsum_vel_z_ema",
        "f_micro_shift_bps_vel_z_ema",
    ]

    def merge(extra: list[str]) -> list[str]:
        out = list(base)
        for c in extra:
            if c not in out:
                out.append(c)
        return out

    return {
        "base": base,
        "base+F": merge(add_f),
        "base+G": merge(add_g),
        "base+H": merge(add_h),
        "base+FGH": merge(add_f + add_g + add_h),
    }


def _get_bucket(mod, ex: dict[str, Any]) -> str:
    scen = ex.get("scenario")
    scen = mod._scenario_norm((scen or ""))
    return str(mod._bucket_from_scenario(scen) or "unknown")


def _fit_predict_oof(mod, ex: list[dict[str, Any]], feature_cols: list[str], args) -> dict[str, Any]:
    # Mirrors train_edge_stack_v1_oof.py main() training loop,
    # but returns OOF predictions for ablation slicing.
    base_feature_names = mod._collect_base_feature_names(feature_cols)

    # Load feature transforms (same contract as train_edge_stack_v1_oof.py)
    transforms = args._feature_transforms or {}

    # Fit / load robust scaler
    scaler: dict[str, Any] = {}
    if int(args.with_robust_scaler) == 1:
        scaler = mod._fit_robust_scaler(ex, base_feature_names, feature_cols)

    # Build X, y, ts
    X: list[list[float]] = []
    y: list[int] = []
    ts: list[int] = []
    buckets: list[str] = []
    for r in ex:
        X.append(mod.build_feature_row(r, base_feature_names, feature_cols, transforms, scaler, strict=int(args.strict) == 1))
        y.append(int(mod._get_label(r)))
        ts.append(int(r.get("ts_ms") or 0))
        buckets.append(_get_bucket(mod, r))

    # OOF base
    splitter = mod.PurgedEmbargoTimeSeriesSplit(
        n_splits=int(args.n_splits),
        purge_ms=int(args.purge_ms),
        embargo_ms=int(args.embargo_ms),
    )

    import numpy as np
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression

    X_np = np.asarray(X, dtype=np.float32)
    y_np = np.asarray(y, dtype=np.int32)

    oof_lr = np.zeros(len(ex), dtype=np.float32)
    oof_gbdt = np.zeros(len(ex), dtype=np.float32)
    for fold, (tr, te) in enumerate(splitter.split(ts)):
        tr = list(tr)
        te = list(te)

        X_tr = X_np[tr]
        y_tr = y_np[tr]
        X_te = X_np[te]

        lr = LogisticRegression(
            max_iter=500,
            C=float(args.lr_C),
            solver="lbfgs",
            class_weight="balanced" if int(args.lr_class_weight_balanced) == 1 else None,
            random_state=int(args.seed) + fold,
        )
        lr.fit(X_tr, y_tr)
        p_lr = lr.predict_proba(X_te)[:, 1]

        gbdt = HistGradientBoostingClassifier(
            learning_rate=float(args.gbdt_lr),
            max_depth=int(args.gbdt_max_depth),
            max_leaf_nodes=int(args.gbdt_max_leaf_nodes),
            min_samples_leaf=int(args.gbdt_min_samples_leaf),
            l2_regularization=float(args.gbdt_l2),
            max_iter=int(args.gbdt_max_iter),
            random_state=int(args.seed) + 1000 + fold,
        )
        gbdt.fit(X_tr, y_tr)
        p_gbdt = gbdt.predict_proba(X_te)[:, 1]

        oof_lr[te] = p_lr.astype(np.float32)
        oof_gbdt[te] = p_gbdt.astype(np.float32)

    # Meta fit on OOF
    Z = np.stack([oof_lr, oof_gbdt], axis=1)
    meta = LogisticRegression(
        max_iter=500,
        C=float(args.meta_C),
        solver="lbfgs",
        class_weight="balanced" if int(args.meta_class_weight_balanced) == 1 else None,
        random_state=int(args.seed) + 4242,
    )
    meta.fit(Z, y_np)
    p_meta = meta.predict_proba(Z)[:, 1]

    return {
        "y": y,
        "p_lr": [float(x) for x in oof_lr.tolist()],
        "p_gbdt": [float(x) for x in oof_gbdt.tolist()],
        "p_meta": [float(x) for x in p_meta.tolist()],
        "ts_ms": ts,
        "bucket": buckets,
        "feature_cols_n": int(len(feature_cols)),
        "derived_present": {k: any((k in (r.get("indicators") or {})) for r in ex) for k in FGH_NUMERIC_KEYS},
    }


def _slice_metrics(y: list[int], p: list[float]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "n": int(len(y)),
        "pos_rate": float(sum(int(v) for v in y)) / float(len(y) or 1),
        "auc_roc": _safe_auc_roc(y, p),
        "p_mean": float(sum(float(x) for x in p)) / float(len(p) or 1),
        "prec_top_1pct": _topk_precision(y, p, 0.01),
        "prec_top_5pct": _topk_precision(y, p, 0.05),
    }
    return out


def _per_bucket_report(res: dict[str, Any]) -> dict[str, Any]:
    y = res["y"]
    p = res["p_meta"]
    b = res["bucket"]
    buckets = sorted(set(b))
    rep: dict[str, Any] = {"overall": _slice_metrics(y, p), "by_bucket": {}}
    for bb in buckets:
        idx = [i for i in range(len(b)) if b[i] == bb]
        rep["by_bucket"][bb] = _slice_metrics([y[i] for i in idx], [p[i] for i in idx])
    return rep


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_jsonl", required=True)
    ap.add_argument("--feature_cols_json", required=True)
    ap.add_argument("--out_json", default="")
    ap.add_argument("--n_splits", type=int, default=5)
    ap.add_argument("--purge_ms", type=int, default=0)
    ap.add_argument("--embargo_ms", type=int, default=0)
    ap.add_argument("--seed", type=int, default=1337)
    ap.add_argument("--p_min", type=float, default=0.55)
    ap.add_argument("--strict", type=int, default=0)
    ap.add_argument("--max_abs_clip", type=float, default=30.0)

    # Match train_edge_stack_v1_oof.py knobs for comparable deltas
    ap.add_argument("--with_robust_scaler", type=int, default=1)
    ap.add_argument("--feature_transforms_json", default="")

    ap.add_argument("--lr_C", type=float, default=1.0)
    ap.add_argument("--lr_class_weight_balanced", type=int, default=1)

    ap.add_argument("--gbdt_lr", type=float, default=0.08)
    ap.add_argument("--gbdt_max_depth", type=int, default=4)
    ap.add_argument("--gbdt_max_leaf_nodes", type=int, default=31)
    ap.add_argument("--gbdt_min_samples_leaf", type=int, default=120)
    ap.add_argument("--gbdt_l2", type=float, default=2.0)
    ap.add_argument("--gbdt_max_iter", type=int, default=260)

    ap.add_argument("--meta_C", type=float, default=0.8)
    ap.add_argument("--meta_class_weight_balanced", type=int, default=1)
    args = ap.parse_args()

    from ml_analysis.tools import train_edge_stack_v1_oof as mod

    # Load transforms once (optional)
    args._feature_transforms = {}
    if args.feature_transforms_json:
        with open(args.feature_transforms_json, encoding="utf-8") as f:
            ft = json.load(f)
        if isinstance(ft, dict):
            args._feature_transforms = ft

    rows = mod._load_jsonl(args.data_jsonl)
    with open(args.feature_cols_json, encoding="utf-8") as f:
        base_cols = json.load(f)
    if not isinstance(base_cols, list) or not all(isinstance(x, str) for x in base_cols):
        raise SystemExit("feature_cols_json must be a JSON array of strings")

    # build examples with the same filtering logic as training
    ex: list[dict[str, Any]] = []
    for r in rows:
        try:
            p = float(r.get("p") or 0.0)
        except Exception:
            p = 0.0
        try:
            _ = int(r.get("ts_ms") or 0)
        except Exception:
            continue
        y = mod._get_label(r)
        if y is None:
            continue
        if p < float(args.p_min):
            continue
        ex.append(r)
    ex.sort(key=lambda rr: int(rr.get("ts_ms") or 0))

    variants = _variant_feature_cols([str(x) for x in base_cols])

    results: dict[str, Any] = {"meta": {"p_min": float(args.p_min), "n": int(len(ex))}, "variants": {}}
    for name, cols in variants.items():
        res = _fit_predict_oof(mod, ex, cols, args)
        rep = _per_bucket_report(res)
        results["variants"][name] = {"oof": res, "report": rep}

    # Print delta table vs base
    base_rep = results["variants"]["base"]["report"]
    base_over = base_rep["overall"]
    base_by = base_rep["by_bucket"]

    # Collect union of buckets
    bucket_set = set(base_by.keys())
    for v in results["variants"].values():
        bucket_set |= set(v["report"]["by_bucket"].keys())
    buckets = sorted(bucket_set)

    print("\n# Ablation: F/G/H (meta OOF) — deltas vs baseline\n")
    print(f"Samples used (after p_min): n={results['meta']['n']}")
    print("\n## Overall\n")
    print("variant | auc_roc | Δauc | prec@5% | Δprec@5%")
    print("---|---:|---:|---:|---:")
    for name, v in results["variants"].items():
        over = v["report"]["overall"]
        dau = float(over.get("auc_roc", float("nan"))) - float(base_over.get("auc_roc", float("nan")))
        dpk = float(over.get("prec_top_5pct", float("nan"))) - float(base_over.get("prec_top_5pct", float("nan")))
        print(f"{name} | {_fmt(over.get('auc_roc'))} | {_fmt(dau)} | {_fmt(over.get('prec_top_5pct'))} | {_fmt(dpk)}")

    print("\n## By bucket (trend/range/other)\n")
    print("bucket | variant | auc_roc | Δauc | prec@5% | Δprec@5% | n")
    print("---|---|---:|---:|---:|---:|---:")
    for bb in buckets:
        b0 = base_by.get(bb, {})
        for name, v in results["variants"].items():
            cur = v["report"]["by_bucket"].get(bb, {})
            if not cur:
                continue
            dau = float(cur.get("auc_roc", float("nan"))) - float(b0.get("auc_roc", float("nan")))
            dpk = float(cur.get("prec_top_5pct", float("nan"))) - float(b0.get("prec_top_5pct", float("nan")))
            print(
                f"{bb} | {name} | {_fmt(cur.get('auc_roc'))} | {_fmt(dau)} | {_fmt(cur.get('prec_top_5pct'))} | {_fmt(dpk)} | {int(cur.get('n') or 0)}"
            )

    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\n[OK] wrote {args.out_json}")


if __name__ == "__main__":
    main()
