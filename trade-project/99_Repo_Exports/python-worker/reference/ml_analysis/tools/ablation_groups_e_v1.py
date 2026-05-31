from __future__ import annotations

"""Drop-group ablation for E-block (Hawkes/VPIN/limit-add) + auto-denylist.

Offline only (no runtime changes).

Outputs (out_dir)
  - ablation_overall.csv
  - ablation_by_regime.csv
  - importance_e_features.csv
  - denylist_autogen.json
  - denylist_autogen.txt
"""


import argparse
import csv
import datetime as _dt
import json
import math
import os
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier  # type: ignore
from sklearn.impute import SimpleImputer  # type: ignore
from sklearn.linear_model import LogisticRegression  # type: ignore
from sklearn.metrics import log_loss, roc_auc_score  # type: ignore
from sklearn.pipeline import Pipeline  # type: ignore
from sklearn.preprocessing import StandardScaler  # type: ignore

from ml_analysis.common.feature_groups_e_v1 import build_e_groups, group_features, normalize_feature_key


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        v = float(x)
        if not math.isfinite(v):
            return d
        return float(v)
    except Exception:
        return d


def _normalize_regime(x: Any) -> str:
    s = (x or "").strip().lower()
    if s in ("trend", "range", "other"):
        return s
    if s in ("flat", "consolidation"):
        return "range"
    if "trend" in s:
        return "trend"
    if "range" in s:
        return "range"
    return "other" if s else "other"


def _precision_at_top_frac(y_true: np.ndarray, p: np.ndarray, top_frac: float) -> float:
    y = np.asarray(y_true, dtype=np.int8)
    s = np.asarray(p, dtype=np.float64)
    n = len(y)
    if n == 0:
        return 0.0
    k = int(max(1, round(float(top_frac) * n)))
    idx = np.argsort(-s)[:k]
    return float(y[idx].mean()) if k > 0 else 0.0


@dataclass
class Fold:
    train_idx: np.ndarray
    test_idx: np.ndarray


def make_time_folds(ts_ms: np.ndarray, *, n_splits: int, purge_ms: int, min_train_rows: int) -> list[Fold]:
    t = np.asarray(ts_ms, dtype=np.int64)
    order = np.argsort(t)
    n = len(order)
    if n < max(5_000, min_train_rows):
        return []

    n_splits = int(max(2, n_splits))
    bounds = np.linspace(0, n, n_splits + 1).astype(int)
    folds: list[Fold] = []
    for i in range(1, len(bounds) - 1):
        test_lo = int(bounds[i])
        test_hi = int(bounds[i + 1])
        if test_hi - test_lo < 100:
            continue
        test_idx = order[test_lo:test_hi]
        test_start_ts = int(t[test_idx].min())
        train_cut_ts = test_start_ts - int(purge_ms)
        train_idx = order[t[order] < train_cut_ts]
        if len(train_idx) < min_train_rows:
            continue
        folds.append(Fold(train_idx=train_idx, test_idx=test_idx))
    return folds


def _fit_model(model_name: str, random_state: int) -> Pipeline:
    m = (model_name or "gbdt").strip().lower()
    if m in ("lr", "logreg", "logit"):
        return Pipeline(
            steps=[
                ("imp", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler(with_mean=True)),
                (
                    "clf",
                    LogisticRegression(
                        solver="lbfgs",
                        max_iter=800,
                        class_weight="balanced",
                        random_state=random_state,
                    )
                )
            ]
        )
    if m in ("gbdt", "hgb", "hist"):
        return Pipeline(
            steps=[
                ("imp", SimpleImputer(strategy="median")),
                (
                    "clf",
                    HistGradientBoostingClassifier(
                        max_depth=6,
                        max_leaf_nodes=31,
                        learning_rate=0.06,
                        max_iter=300,
                        random_state=random_state,
                    )
                )
            ]
        )
    raise SystemExit(f"Unknown --model={model_name!r}. Use lr|gbdt")


def _predict_proba(model: Pipeline, X: np.ndarray) -> np.ndarray:
    p = model.predict_proba(X)
    return np.asarray(p[:, 1], dtype=np.float64)


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _write_csv(path: str, rows: list[dict[str, Any]], fieldnames: Sequence[str]) -> None:
    _ensure_dir(os.path.dirname(os.path.abspath(path)) or ".")
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(fieldnames))
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})


def _load_df(path: str) -> pd.DataFrame:
    p = str(path)
    if p.endswith(".parquet"):
        return pd.read_parquet(p)
    if p.endswith(".csv"):
        return pd.read_csv(p)
    if p.endswith(".jsonl") or p.endswith(".ndjson") or p.endswith(".json"):
        return pd.read_json(p, lines=True)
    raise SystemExit(f"Unsupported data_path format: {p}")


def _read_meta(meta_json: str) -> dict[str, Any]:
    obj = json.loads(open(meta_json, encoding="utf-8").read())
    if not isinstance(obj, dict):
        raise SystemExit("meta_json must be a dict")
    return obj


def _build_key_to_col(meta: dict[str, Any]) -> dict[str, str]:
    col_map = meta.get("column_map")
    if isinstance(col_map, dict) and col_map:
        out: dict[str, str] = {}
        for fn, col in col_map.items():
            out[normalize_feature_key(str(fn))] = str(col)
        return out
    feat_names = list(meta.get("feature_names") or [])
    col_names = list(meta.get("column_names") or [])
    out: dict[str, str] = {}
    if feat_names and col_names and len(feat_names) == len(col_names):
        for fn, col in zip(feat_names, col_names):
            out[normalize_feature_key(str(fn))] = str(col)
    return out


def _metrics(y: np.ndarray, p: np.ndarray, top_frac: float) -> dict[str, float]:
    yb = np.asarray(y, dtype=np.int8)
    pb = np.clip(np.asarray(p, dtype=np.float64), 1e-9, 1.0 - 1e-9)
    auc = float(roc_auc_score(yb, pb)) if (yb.sum() > 0 and yb.sum() < len(yb)) else 0.5
    ll = float(log_loss(yb, pb, labels=[0, 1]))
    brier = float(np.mean((pb - yb) ** 2))
    ptop = _precision_at_top_frac(yb, pb, top_frac)
    return {"auc": auc, "logloss": ll, "brier": brier, "prec_top": ptop}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_path", required=True)
    ap.add_argument("--meta_json", required=True)
    ap.add_argument("--out_dir", required=True)

    ap.add_argument("--label_col", default="y")
    ap.add_argument("--ts_col", default="ts_ms")
    ap.add_argument("--regime_col", default="scenario_v4")
    ap.add_argument("--model", default="gbdt", choices=["lr", "gbdt"])

    ap.add_argument("--n_splits", type=int, default=5)
    ap.add_argument("--purge_ms", type=int, default=60_000)
    ap.add_argument("--min_train_rows", type=int, default=10_000)
    ap.add_argument("--top_frac", type=float, default=0.05)

    ap.add_argument(
        "--groups",
        default="E_vpin,E_limit_add,E_hawkes_split,E_hawkes_legacy,E_lambda_alias",
        help="comma-separated group names to ablate",
    )

    ap.add_argument("--perm_max_rows_per_fold", type=int, default=50_000)
    ap.add_argument("--deny_min_perm_auc_drop", type=float, default=0.0005)
    ap.add_argument("--deny_max_features", type=int, default=64)

    args = ap.parse_args()

    out_dir = str(args.out_dir)
    _ensure_dir(out_dir)

    meta = _read_meta(str(args.meta_json))
    key_to_col = _build_key_to_col(meta)
    if not key_to_col:
        raise SystemExit("meta_json missing column_map/feature_names")

    df = _load_df(str(args.data_path))
    if str(args.label_col) not in df.columns:
        raise SystemExit(f"label_col={args.label_col!r} not found")
    if str(args.ts_col) not in df.columns:
        raise SystemExit(f"ts_col={args.ts_col!r} not found")

    regime_col = str(args.regime_col)
    if regime_col not in df.columns and "scenario" in df.columns:
        regime_col = "scenario"

    # Full feature columns
    all_keys_sorted = sorted(key_to_col.keys())
    full_cols: list[str] = []
    full_keys: list[str] = []
    for k in all_keys_sorted:
        c = key_to_col.get(k)
        if c and c in df.columns:
            full_cols.append(c)
            full_keys.append(k)
    if len(full_cols) < 10:
        raise SystemExit("Too few feature columns found; check meta_json vs dataset")

    # E groups
    groups = build_e_groups()
    grouped = group_features(full_keys, groups)
    want_groups = [s.strip() for s in str(args.groups).split(",") if s.strip()]
    grouped = {k: v for (k, v) in grouped.items() if k in want_groups}
    group_cols: dict[str, list[str]] = {}
    for g, keys in grouped.items():
        cols = []
        for k in sorted(keys):
            c = key_to_col.get(k)
            if c and c in df.columns:
                cols.append(c)
        group_cols[g] = cols

    ts = df[str(args.ts_col)].astype("int64").to_numpy()
    folds = make_time_folds(ts, n_splits=int(args.n_splits), purge_ms=int(args.purge_ms), min_train_rows=int(args.min_train_rows))
    if not folds:
        raise SystemExit("No folds created (too few rows or min_train_rows too high)")

    y_all = df[str(args.label_col)].to_numpy()
    y_all = np.asarray([int(_safe_float(v, 0.0) > 0.5) for v in y_all], dtype=np.int8)
    regimes = np.asarray([_normalize_regime(x) for x in df[regime_col].to_numpy()], dtype=object) if regime_col in df.columns else np.asarray(["other"] * len(df), dtype=object)

    variants: list[tuple[str, list[str]]] = [("full", full_cols)]
    for g, cols in group_cols.items():
        if not cols:
            continue
        drop = set(cols)
        kept = [c for c in full_cols if c not in drop]
        variants.append((f"drop_{g}", kept))

    overall_rows: list[dict[str, Any]] = []
    by_regime_rows: list[dict[str, Any]] = []

    baseline_metrics: dict[str, float] | None = None
    baseline_by_regime: dict[str, dict[str, float]] = {}
    baseline_fold_models: list[tuple[Pipeline, np.ndarray]] = []
    baseline_fold_cols: list[str] | None = None

    for v_name, cols in variants:
        X = df[cols].to_numpy(dtype=np.float64, copy=False)
        p_oof = np.full((len(df),), np.nan, dtype=np.float64)

        for fi, fold in enumerate(folds):
            tr, te = fold.train_idx, fold.test_idx
            X_tr = X[tr]
            y_tr = y_all[tr]
            X_te = X[te]
            y_te = y_all[te]
            if y_tr.sum() == 0 or y_tr.sum() == len(y_tr):
                continue
            model = _fit_model(str(args.model), random_state=7 + fi)
            model.fit(X_tr, y_tr)
            p = _predict_proba(model, X_te)
            p_oof[te] = p
            if v_name == "full":
                baseline_fold_models.append((model, te))

        mask = np.isfinite(p_oof)
        y = y_all[mask]
        p = p_oof[mask]
        m = _metrics(y, p, float(args.top_frac))

        if v_name == "full":
            baseline_metrics = dict(m)
            baseline_fold_cols = list(cols)

        row = {
            "variant": v_name,
            "n": int(mask.sum()),
            "auc": m["auc"],
            "logloss": m["logloss"],
            "brier": m["brier"],
            "prec_top": m["prec_top"],
        }
        if baseline_metrics is not None:
            row["d_auc"] = m["auc"] - baseline_metrics["auc"]
            row["d_logloss"] = m["logloss"] - baseline_metrics["logloss"]
            row["d_brier"] = m["brier"] - baseline_metrics["brier"]
            row["d_prec_top"] = m["prec_top"] - baseline_metrics["prec_top"]
        overall_rows.append(row)

        for g in ("trend", "range", "other"):
            msk_g = mask & (regimes == g)
            if int(msk_g.sum()) < 200:
                continue
            mg = _metrics(y_all[msk_g], p_oof[msk_g], float(args.top_frac))
            if v_name == "full":
                baseline_by_regime[g] = dict(mg)
            rr = {
                "variant": v_name,
                "regime": g,
                "n": int(msk_g.sum()),
                "auc": mg["auc"],
                "logloss": mg["logloss"],
                "brier": mg["brier"],
                "prec_top": mg["prec_top"],
            }
            if g in baseline_by_regime:
                rr["d_auc"] = mg["auc"] - baseline_by_regime[g]["auc"]
                rr["d_logloss"] = mg["logloss"] - baseline_by_regime[g]["logloss"]
                rr["d_brier"] = mg["brier"] - baseline_by_regime[g]["brier"]
                rr["d_prec_top"] = mg["prec_top"] - baseline_by_regime[g]["prec_top"]
            by_regime_rows.append(rr)

    _write_csv(
        os.path.join(out_dir, "ablation_overall.csv"),
        overall_rows,
        ["variant", "n", "auc", "d_auc", "logloss", "d_logloss", "brier", "d_brier", "prec_top", "d_prec_top"],
    )
    _write_csv(
        os.path.join(out_dir, "ablation_by_regime.csv"),
        by_regime_rows,
        ["variant", "regime", "n", "auc", "d_auc", "logloss", "d_logloss", "brier", "d_brier", "prec_top", "d_prec_top"],
    )

    if baseline_fold_cols is None or not baseline_fold_models:
        raise SystemExit("Baseline folds missing; cannot compute permutation importance")

    # E feature columns in baseline
    e_keys: list[str] = sorted({k for ks in grouped.values() for k in ks})
    e_cols: list[str] = []
    e_col_idx: list[int] = []
    for k in e_keys:
        col = key_to_col.get(k)
        if not col:
            continue
        if col not in baseline_fold_cols:
            continue
        e_cols.append(col)
        e_col_idx.append(baseline_fold_cols.index(col))

    imp_sum = np.zeros((len(e_cols),), dtype=np.float64)
    imp_sum2 = np.zeros((len(e_cols),), dtype=np.float64)
    imp_n = np.zeros((len(e_cols),), dtype=np.int64)

    for fi, (model, te_idx) in enumerate(baseline_fold_models):
        te_idx = np.asarray(te_idx, dtype=np.int64)
        if len(te_idx) < 500:
            continue
        if len(te_idx) > int(args.perm_max_rows_per_fold):
            rng = np.random.RandomState(7 + fi)
            te_idx = rng.choice(te_idx, size=int(args.perm_max_rows_per_fold), replace=False)

        X_te = df[baseline_fold_cols].to_numpy(dtype=np.float64, copy=False)[te_idx]
        y_te = y_all[te_idx]
        if y_te.sum() == 0 or y_te.sum() == len(y_te):
            continue
        p_base = _predict_proba(model, X_te)
        base_auc = float(roc_auc_score(y_te, np.clip(p_base, 1e-9, 1.0 - 1e-9)))

        for j, col_i in enumerate(e_col_idx):
            X_perm = X_te.copy()
            rng = np.random.RandomState(13_000 + fi * 997 + j)
            rng.shuffle(X_perm[:, col_i])
            p_perm = _predict_proba(model, X_perm)
            perm_auc = float(roc_auc_score(y_te, np.clip(p_perm, 1e-9, 1.0 - 1e-9)))
            imp = base_auc - perm_auc
            imp_sum[j] += imp
            imp_sum2[j] += imp * imp
            imp_n[j] += 1

    imp_rows: list[dict[str, Any]] = []
    for k, col, s, s2, n in zip(e_keys, e_cols, imp_sum, imp_sum2, imp_n):
        if int(n) <= 0:
            continue
        mu = float(s / n)
        var = float(max(0.0, (s2 / n) - mu * mu))
        sd = float(math.sqrt(var))
        imp_rows.append({"feature": k, "column": col, "perm_auc_drop_mean": mu, "perm_auc_drop_std": sd, "n_folds": int(n)})

    imp_rows.sort(key=lambda r: float(r.get("perm_auc_drop_mean", 0.0)))
    _write_csv(
        os.path.join(out_dir, "importance_e_features.csv"),
        imp_rows,
        ["feature", "column", "perm_auc_drop_mean", "perm_auc_drop_std", "n_folds"],
    )

    deny_keys: list[str] = []
    for r in imp_rows:
        if len(deny_keys) >= int(args.deny_max_features):
            break
        if float(r.get("perm_auc_drop_mean", 0.0)) <= float(args.deny_min_perm_auc_drop):
            deny_keys.append((r.get("feature")))

    deny_obj = {
        "ver": "v1",
        "updated_utc": _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "deny_num": deny_keys,
        "deny_bool": [],
        "notes": "autogen from ablation_groups_e_v1 perm AUC-drop (E block)",
    }
    with open(os.path.join(out_dir, "denylist_autogen.json"), "w", encoding="utf-8") as f:
        json.dump(deny_obj, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "denylist_autogen.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(deny_keys) + ("\n" if deny_keys else ""))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
