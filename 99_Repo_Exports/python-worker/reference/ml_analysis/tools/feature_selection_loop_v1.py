"""Minimal feature selection loop: importance + stability by regimes/sessions.

Goal
  - быстрый "санити"-контур для отсечения шумовых добавлений в schema v4_of/v5_of
  - один запуск → отчет: global importance + stability по режимам (trend/range/other)
    и по часам UTC (hour buckets)

Input
  Dataset, созданный build_dataset_from_inputs_outcomes_v2.py с:
    --emit-wide-cols=1 --schema-ver=v4_of|v5_of
  Форматы: .parquet (предпочт.), .csv, .ndjson/.jsonl
  + sidecar meta.json (out.*.meta.json) с feature_names/column_names.

Outputs (out_dir)
  - summary.json                    (качество, counts)
  - importance_global.csv           (global importance, ranks)
  - importance_by_regime.csv        (trend/range/other)
  - importance_by_hour.csv          (0..23)
  - stability_table.csv             (сводка: mean/std/cv + flags)
  - perf_by_regime.csv, perf_by_hour.csv
  - report.md                       (читаемый отчет)

Notes
  - SHAP: опционально. Если shap не установлен/не поддерживает модель —
    используется permutation importance (AUC drop).
  - Детерминизм: сортировка по ts_ms, фиксированный random_state.

Example
  PYTHONPATH=./tick_flow_full:./ml_analysis \
    python3 -m tools.feature_selection_loop_v1 \
      --data_path ./edge_train.parquet \
      --meta_json ./edge_train.parquet.meta.json \
      --out_dir ./fs_loop_out \
      --model gbdt --max_val_rows 200000
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import csv
import json
import math
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


try:
    import numpy as np
except Exception as e:  # pragma: no cover
    raise SystemExit(f"numpy is required: {e}")

try:
    import pandas as pd
except Exception as e:  # pragma: no cover
    raise SystemExit(f"pandas is required: {e}")


# sklearn is optional at import time; we fail with a clean message at runtime
try:
    from sklearn.linear_model import LogisticRegression  # type: ignore
    from sklearn.ensemble import HistGradientBoostingClassifier  # type: ignore
    from sklearn.metrics import roc_auc_score  # type: ignore
except Exception:
    LogisticRegression = None  # type: ignore
    HistGradientBoostingClassifier = None  # type: ignore
    roc_auc_score = None  # type: ignore


# ---------------------------------------------------------------------------
# Centralized schema choices (avoid drift across tools)
# ---------------------------------------------------------------------------
try:
    from tools.schema_choices_v1 import schema_choices as _schema_choices, normalize_schema_ver as _norm_schema_ver  # type: ignore
except Exception:  # pragma: no cover
    from ml_analysis.tools.schema_choices_v1 import schema_choices as _schema_choices, normalize_schema_ver as _norm_schema_ver  # type: ignore


def _now_ms() -> int:
    return get_ny_time_millis()


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return float(d)
        v = float(x)
        if not math.isfinite(v):
            return float(d)
        return float(v)
    except Exception:
        return float(d)


def _safe_int(x: Any, d: int = 0) -> int:
    try:
        if x is None:
            return int(d)
        if isinstance(x, bool):
            return int(d)
        return int(float(x))
    except Exception:
        return int(d)


def _utc_hour(ts_ms: int) -> int:
    try:
        return int((int(ts_ms) // 1000) % 86400) // 3600
    except Exception:
        return 0


def _normalize_regime(x: Any) -> str:
    s = str(x or "").strip().lower()
    if s in ("trend", "range", "other"):
        return s
    # common aliases
    if s in ("flat", "consolidation"):
        return "range"
    return "other" if s else "other"


def _auc(y_true: np.ndarray, p: np.ndarray) -> float:
    # Prefer sklearn if available; fallback to rank-based AUC.
    if roc_auc_score is not None:
        try:
            return float(roc_auc_score(y_true, p))
        except Exception:
            pass

    y = np.asarray(y_true, dtype=np.int8)
    s = np.asarray(p, dtype=np.float64)
    # If all same class, AUC undefined.
    if y.sum() == 0 or y.sum() == len(y):
        return 0.5
    order = np.argsort(s)
    y_ord = y[order]
    # rank sum for positives
    ranks = np.arange(1, len(y_ord) + 1)
    rank_sum_pos = float((ranks * y_ord).sum())
    n_pos = float(y_ord.sum())
    n_neg = float(len(y_ord) - y_ord.sum())
    # Mann–Whitney U
    u = rank_sum_pos - n_pos * (n_pos + 1.0) / 2.0
    return float(u / (n_pos * n_neg))


def _brier(y_true: np.ndarray, p: np.ndarray) -> float:
    y = np.asarray(y_true, dtype=np.float64)
    s = np.asarray(p, dtype=np.float64)
    s = np.clip(s, 1e-9, 1.0 - 1e-9)
    return float(np.mean((s - y) ** 2))


@dataclass
class Split:
    train_idx: np.ndarray
    val_idx: np.ndarray


def time_split(
    ts_ms: np.ndarray,
    *,
    val_frac: float = 0.2,
    purge_ms: int = 300_000,
) -> Split:
    """Deterministic time split with a purge band around the boundary."""
    t = np.asarray(ts_ms, dtype=np.int64)
    order = np.argsort(t)
    n = len(order)
    if n == 0:
        return Split(train_idx=np.array([], dtype=np.int64), val_idx=np.array([], dtype=np.int64))
    k = int(max(1, min(n - 1, round(n * (1.0 - float(val_frac))))))
    boundary_ts = int(t[order[k]])
    # purge: drop records within [boundary-purge, boundary+purge]
    lo = boundary_ts - int(purge_ms)
    hi = boundary_ts + int(purge_ms)
    train_mask = t < lo
    val_mask = t > hi
    return Split(train_idx=np.where(train_mask)[0], val_idx=np.where(val_mask)[0])


def _fit_model(
    model_name: str,
    X_tr: np.ndarray,
    y_tr: np.ndarray,
    *,
    random_state: int = 7,
):
    if LogisticRegression is None or HistGradientBoostingClassifier is None:
        raise SystemExit("scikit-learn is required (LogisticRegression, HistGradientBoostingClassifier)")

    m = str(model_name or "gbdt").strip().lower()
    if m in ("lr", "logreg", "logit"):
        # Balanced makes it more robust across symbols.
        model = LogisticRegression(
            solver="lbfgs",
            max_iter=500,
            n_jobs=None,
            class_weight="balanced",
            random_state=random_state,
        )
        model.fit(X_tr, y_tr)
        return model

    if m in ("gbdt", "hgb", "hist"):
        model = HistGradientBoostingClassifier(
            max_depth=6,
            max_leaf_nodes=31,
            learning_rate=0.06,
            max_iter=300,
            random_state=random_state,
        )
        model.fit(X_tr, y_tr)
        return model

    raise SystemExit(f"Unknown --model={model_name!r}. Use lr|gbdt")


def _predict_proba(model: Any, X: np.ndarray) -> np.ndarray:
    # supports sklearn-like estimators
    if hasattr(model, "predict_proba"):
        p = model.predict_proba(X)
        return np.asarray(p[:, 1], dtype=np.float64)
    # fallback: decision_function -> sigmoid
    if hasattr(model, "decision_function"):
        z = np.asarray(model.decision_function(X), dtype=np.float64)
        return 1.0 / (1.0 + np.exp(-z))
    raise RuntimeError("model has no predict_proba/decision_function")


def permutation_importance_auc_drop(
    model: Any,
    X: np.ndarray,
    y: np.ndarray,
    *,
    feature_names: Sequence[str],
    n_repeats: int = 3,
    max_features: Optional[int] = None,
    seed: int = 7,
) -> Dict[str, float]:
    """Permutation importance as AUC drop.

    importance[f] = auc_base - mean(auc_perm)
    """
    rng = np.random.default_rng(int(seed))
    X0 = np.asarray(X, dtype=np.float64)
    y0 = np.asarray(y, dtype=np.int8)
    p0 = _predict_proba(model, X0)
    auc0 = _auc(y0, p0)

    n = X0.shape[0]
    d = X0.shape[1]
    idxs = list(range(d))
    if max_features is not None and int(max_features) > 0 and len(idxs) > int(max_features):
        idxs = idxs[: int(max_features)]

    out: Dict[str, float] = {}
    for j in idxs:
        aucs: List[float] = []
        for _ in range(int(max(1, n_repeats))):
            Xp = X0.copy()
            perm = rng.permutation(n)
            Xp[:, j] = Xp[perm, j]
            pp = _predict_proba(model, Xp)
            aucs.append(_auc(y0, pp))
        drop = float(auc0 - float(np.mean(aucs)))
        out[str(feature_names[j])] = drop
    return out


def _try_shap_importance(
    model: Any,
    X: np.ndarray,
    *,
    feature_names: Sequence[str],
    max_rows: int = 20000,
    seed: int = 7,
) -> Optional[Dict[str, float]]:
    """Return mean(|SHAP|) per feature if shap is available; otherwise None."""
    try:
        import shap  # type: ignore
    except Exception:
        return None

    rng = np.random.default_rng(int(seed))
    X0 = np.asarray(X, dtype=np.float64)
    if X0.shape[0] == 0:
        return None
    if X0.shape[0] > int(max_rows):
        idx = rng.choice(X0.shape[0], size=int(max_rows), replace=False)
        Xs = X0[idx]
    else:
        Xs = X0

    try:
        explainer = shap.Explainer(model, Xs)
        sv = explainer(Xs)
        vals = np.asarray(sv.values, dtype=np.float64)
        if len(vals.shape) == 3:
            # (n, d, 2) -> take class=1
            vals = vals[:, :, -1]
        mean_abs = np.mean(np.abs(vals), axis=0)
        out = {str(feature_names[i]): float(mean_abs[i]) for i in range(len(feature_names))}
        return out
    except Exception:
        return None


def _write_csv(path: str, rows: List[Dict[str, Any]], fieldnames: Sequence[str]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(fieldnames))
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})


def _fmt_pct(x: float) -> str:
    return f"{100.0 * float(x):.2f}%"


def _fmt_opt(x: Any, fmt: str) -> str:
    if x is None:
        return ""
    try:
        return format(float(x), fmt)
    except Exception:
        return ""


def _topk(d: Dict[str, float], k: int = 30) -> List[Tuple[str, float]]:
    return sorted([(a, float(b)) for a, b in d.items()], key=lambda t: (-t[1], t[0]))[: int(k)]


def main(argv: Optional[Sequence[str]] = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_path", required=True, help=".parquet/.csv/.ndjson/.jsonl")
    ap.add_argument("--meta_json", default="", help="sidecar meta.json; optional if --schema_ver is set")
    ap.add_argument(
        "--schema_ver",
        default="",
        choices=_schema_choices(include_empty=True),
        help="use FeatureRegistry schema if meta_json not provided (e.g. v7_of_stable)",
    )
    ap.add_argument("--out_dir", required=True)

    ap.add_argument("--label_col", default="y")
    ap.add_argument("--ts_col", default="ts_ms")
    ap.add_argument("--regime_col", default="scenario_v4")
    ap.add_argument("--model", default="gbdt", choices=["gbdt", "lr"], help="primary model for importance")

    ap.add_argument("--val_frac", type=float, default=0.2)
    ap.add_argument("--purge_ms", type=int, default=300_000)
    ap.add_argument("--max_val_rows", type=int, default=250_000)
    ap.add_argument("--max_features", type=int, default=0, help="0=all; else cap #features for permutation")
    ap.add_argument("--n_repeats", type=int, default=3)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--min_group_rows", type=int, default=1500)

    args = ap.parse_args(list(argv) if argv is not None else None)

    out_dir = str(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    # meta.json or FeatureRegistry
    meta: Dict[str, Any] = {}
    feature_names: List[str] = []
    column_names: List[str] = []
    if str(args.meta_json).strip():
        with open(str(args.meta_json), "r", encoding="utf-8") as f:
            meta = json.load(f)
        feature_names = list(meta.get("feature_names") or [])
        column_names = list(meta.get("column_names") or [])
    elif str(args.schema_ver).strip():
        # lazy import to keep this tool runnable without tick_flow_full on PYTHONPATH
        try:
            from core.feature_registry import FeatureRegistry  # type: ignore
        except Exception as e:
            raise SystemExit(f"cannot import FeatureRegistry; set PYTHONPATH to include tick_flow_full: {e}")
        s = FeatureRegistry().get_schema_info(_norm_schema_ver(str(args.schema_ver).strip()))
        meta = {"ver": s.ver, "schema_hash": s.schema_hash}
        feature_names = list(s.feature_names)
        column_names = list(s.column_names)
    else:
        raise SystemExit("Provide --meta_json or --schema_ver")

    if not feature_names or not column_names or len(feature_names) != len(column_names):
        raise SystemExit("feature_names/column_names empty or mismatch")

    # Load dataset
    data_path = str(args.data_path)
    if data_path.endswith(".parquet"):
        df = pd.read_parquet(data_path)
    elif data_path.endswith(".csv"):
        df = pd.read_csv(data_path)
    elif data_path.endswith(".ndjson") or data_path.endswith(".jsonl"):
        rows: List[Dict[str, Any]] = []
        with open(data_path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                rows.append(json.loads(s))
        df = pd.DataFrame(rows)
    else:
        raise SystemExit("Unsupported --data_path format. Use .parquet/.csv/.ndjson/.jsonl")
    # regime fallback (build_edge_stack_dataset_from_redis emits 'scenario')
    if str(args.regime_col) not in df.columns and "scenario" in df.columns:
        args.regime_col = "scenario"

    base_cols = [str(args.label_col), str(args.ts_col), str(args.regime_col)]

    # If wide feature columns are absent but indicators dict exists, expand it.
    if any(c not in df.columns for c in column_names):
        if "indicators" in df.columns:
            ind_df = pd.json_normalize(df["indicators"].tolist()).fillna(0.0)
            # Take only known features and rename to column_names
            have = [f for f in feature_names if f in ind_df.columns]
            ind_df = ind_df.reindex(columns=have).fillna(0.0)
            rename = {feature_names[i]: column_names[i] for i in range(len(feature_names)) if feature_names[i] in ind_df.columns}
            ind_df = ind_df.rename(columns=rename)
            for c in column_names:
                if c not in ind_df.columns:
                    ind_df[c] = 0.0
            df = pd.concat([df.drop(columns=["indicators"], errors="ignore"), ind_df[column_names]], axis=1)
        else:
            missing_feat = [c for c in column_names if c not in df.columns]
            raise SystemExit(f"dataset missing feature columns and no indicators to expand: {missing_feat[:8]} (n={len(missing_feat)})")

    need_cols = base_cols + list(column_names)
    missing = [c for c in need_cols if c not in df.columns]
    if missing:
        raise SystemExit(f"dataset missing columns: {missing[:8]} (n={len(missing)})")

    # Deterministic order
    df = df.sort_values(by=[str(args.ts_col), "sid"] if "sid" in df.columns else [str(args.ts_col)]).reset_index(drop=True)

    # Basic clean
    df[str(args.label_col)] = df[str(args.label_col)].astype(int)
    df[str(args.ts_col)] = df[str(args.ts_col)].astype("int64")
    # fill NaN features with 0 (schema contract: no NaNs on hot path)
    df[column_names] = df[column_names].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    ts = df[str(args.ts_col)].to_numpy(dtype=np.int64)
    y = df[str(args.label_col)].to_numpy(dtype=np.int8)
    X = df[column_names].to_numpy(dtype=np.float64)

    split = time_split(ts, val_frac=float(args.val_frac), purge_ms=int(args.purge_ms))
    if len(split.train_idx) < 1000 or len(split.val_idx) < 1000:
        raise SystemExit(f"not enough data after split: train={len(split.train_idx)} val={len(split.val_idx)}")

    X_tr, y_tr = X[split.train_idx], y[split.train_idx]
    X_va, y_va = X[split.val_idx], y[split.val_idx]

    # Optional cap for faster runs
    max_val = int(args.max_val_rows)
    if max_val > 0 and len(y_va) > max_val:
        rng = np.random.default_rng(int(args.seed))
        idx = rng.choice(len(y_va), size=max_val, replace=False)
        X_va = X_va[idx]
        y_va = y_va[idx]
        ts_va = ts[split.val_idx][idx]
        regime_va = df.loc[split.val_idx, str(args.regime_col)].to_numpy()[idx]
    else:
        ts_va = ts[split.val_idx]
        regime_va = df.loc[split.val_idx, str(args.regime_col)].to_numpy()

    model = _fit_model(str(args.model), X_tr, y_tr, random_state=int(args.seed))
    p_va = _predict_proba(model, X_va)
    auc_va = _auc(y_va, p_va)
    brier_va = _brier(y_va, p_va)

    # Global importance
    shap_imp = _try_shap_importance(model, X_va, feature_names=feature_names, max_rows=20000, seed=int(args.seed))
    if shap_imp is not None:
        global_imp = shap_imp
        global_imp_kind = "shap_mean_abs"
    else:
        global_imp = permutation_importance_auc_drop(
            model,
            X_va,
            y_va,
            feature_names=feature_names,
            n_repeats=int(args.n_repeats),
            max_features=(int(args.max_features) if int(args.max_features) > 0 else None),
            seed=int(args.seed),
        )
        global_imp_kind = "perm_auc_drop"

    # Grouping vectors for stability
    regimes = np.array([_normalize_regime(x) for x in regime_va], dtype=object)
    hours = np.array([_utc_hour(int(t)) for t in ts_va], dtype=np.int16)

    def _group_masks(values: np.ndarray, groups: Sequence[Any]) -> Dict[str, np.ndarray]:
        out: Dict[str, np.ndarray] = {}
        for g in groups:
            out[str(g)] = (values == g)
        return out

    regime_masks = _group_masks(regimes, ["trend", "range", "other"])
    hour_masks = _group_masks(hours, list(range(24)))

    # Per-group performance (AUC/Brier)
    perf_regime: List[Dict[str, Any]] = []
    for g, m in regime_masks.items():
        n = int(m.sum())
        if n < int(args.min_group_rows):
            perf_regime.append({"group": g, "n": n, "pos_rate": float(y_va[m].mean()) if n else 0.0, "auc": None, "brier": None})
            continue
        perf_regime.append({
            "group": g,
            "n": n,
            "pos_rate": float(y_va[m].mean()),
            "auc": float(_auc(y_va[m], p_va[m])),
            "brier": float(_brier(y_va[m], p_va[m])),
        })

    perf_hour: List[Dict[str, Any]] = []
    for h in range(24):
        m = hour_masks[str(h)]
        n = int(m.sum())
        if n < int(args.min_group_rows):
            perf_hour.append({"hour": h, "n": n, "pos_rate": float(y_va[m].mean()) if n else 0.0, "auc": None, "brier": None})
            continue
        perf_hour.append({
            "hour": h,
            "n": n,
            "pos_rate": float(y_va[m].mean()),
            "auc": float(_auc(y_va[m], p_va[m])),
            "brier": float(_brier(y_va[m], p_va[m])),
        })

    # Per-group importance (permutation, stable + comparable)
    # We keep it permutation-based even if global uses SHAP, to have a single stability scale.
    regime_imp: Dict[str, Dict[str, float]] = {}
    for g, m in regime_masks.items():
        n = int(m.sum())
        if n < int(args.min_group_rows):
            continue
        regime_imp[g] = permutation_importance_auc_drop(
            model,
            X_va[m],
            y_va[m],
            feature_names=feature_names,
            n_repeats=max(1, int(args.n_repeats)),
            max_features=(int(args.max_features) if int(args.max_features) > 0 else None),
            seed=int(args.seed),
        )

    hour_imp: Dict[int, Dict[str, float]] = {}
    for h in range(24):
        m = hour_masks[str(h)]
        n = int(m.sum())
        if n < int(args.min_group_rows):
            continue
        hour_imp[h] = permutation_importance_auc_drop(
            model,
            X_va[m],
            y_va[m],
            feature_names=feature_names,
            n_repeats=max(1, int(args.n_repeats)),
            max_features=(int(args.max_features) if int(args.max_features) > 0 else None),
            seed=int(args.seed) + h,
        )

    # Tables
    # Global importance table
    imp_global_rows: List[Dict[str, Any]] = []
    for rank, (fn, val) in enumerate(_topk(global_imp, k=len(global_imp)), start=1):
        imp_global_rows.append({"rank": rank, "feature": fn, "importance": float(val), "kind": global_imp_kind})
    _write_csv(os.path.join(out_dir, "importance_global.csv"), imp_global_rows, ["rank", "feature", "importance", "kind"])

    # Regime importance table
    imp_regime_rows: List[Dict[str, Any]] = []
    for g in ("trend", "range", "other"):
        d = regime_imp.get(g, {})
        for rank, (fn, val) in enumerate(_topk(d, k=len(d)), start=1):
            imp_regime_rows.append({"group": g, "rank": rank, "feature": fn, "importance": float(val)})
    _write_csv(os.path.join(out_dir, "importance_by_regime.csv"), imp_regime_rows, ["group", "rank", "feature", "importance"])

    # Hour importance table
    imp_hour_rows: List[Dict[str, Any]] = []
    for h in sorted(hour_imp.keys()):
        d = hour_imp[h]
        for rank, (fn, val) in enumerate(_topk(d, k=len(d)), start=1):
            imp_hour_rows.append({"hour": int(h), "rank": rank, "feature": fn, "importance": float(val)})
    _write_csv(os.path.join(out_dir, "importance_by_hour.csv"), imp_hour_rows, ["hour", "rank", "feature", "importance"])

    # Stability table (mean/std/cv across available groups)
    def _stats_over(keys: Sequence[str], mp: Dict[Any, Dict[str, float]]) -> Tuple[float, float, float]:
        xs: List[float] = []
        for k in keys:
            d = mp.get(k, {})
            if not d:
                continue
            xs.append(float(d.get(fn, 0.0)))
        if not xs:
            return (0.0, 0.0, 0.0)
        mu = float(np.mean(xs))
        sd = float(np.std(xs))
        cv = float(sd / (abs(mu) + 1e-12))
        return (mu, sd, cv)

    regime_keys = [k for k in ("trend", "range", "other") if k in regime_imp]
    hour_keys = [int(h) for h in sorted(hour_imp.keys())]

    stab_rows: List[Dict[str, Any]] = []
    # Choose global importance column (comparable units may differ; we keep both global and perm_mean)
    # For stability we always use permutation-based aggregates.
    perm_global = permutation_importance_auc_drop(
        model,
        X_va,
        y_va,
        feature_names=feature_names,
        n_repeats=max(1, int(args.n_repeats)),
        max_features=(int(args.max_features) if int(args.max_features) > 0 else None),
        seed=int(args.seed) + 101,
    )

    for fn in feature_names:
        g_imp = float(global_imp.get(fn, 0.0))
        pg = float(perm_global.get(fn, 0.0))
        r_mu, r_sd, r_cv = _stats_over(regime_keys, regime_imp)
        h_mu, h_sd, h_cv = _stats_over([str(h) for h in hour_keys], {str(k): v for k, v in hour_imp.items()})

        # Heuristics (minimal, explainable)
        # - noise: low global perm importance + high instability
        noise = int((pg < 0.001) and ((r_cv > 2.0 and r_sd > 0.0005) or (h_cv > 2.0 and h_sd > 0.0005)))
        strong = int(pg >= 0.005)

        stab_rows.append({
            "feature": fn,
            "global_importance": g_imp,
            "global_perm_auc_drop": pg,
            "regime_mean": r_mu,
            "regime_std": r_sd,
            "regime_cv": r_cv,
            "hour_mean": h_mu,
            "hour_std": h_sd,
            "hour_cv": h_cv,
            "flag_noise": noise,
            "flag_strong": strong,
        })

    stab_rows = sorted(stab_rows, key=lambda r: (-float(r.get("global_perm_auc_drop", 0.0)), -float(r.get("regime_mean", 0.0)), str(r.get("feature"))))
    _write_csv(
        os.path.join(out_dir, "stability_table.csv"),
        stab_rows,
        [
            "feature",
            "global_importance",
            "global_perm_auc_drop",
            "regime_mean",
            "regime_std",
            "regime_cv",
            "hour_mean",
            "hour_std",
            "hour_cv",
            "flag_noise",
            "flag_strong",
        ],
    )

    # perf tables
    _write_csv(os.path.join(out_dir, "perf_by_regime.csv"), perf_regime, ["group", "n", "pos_rate", "auc", "brier"])
    _write_csv(os.path.join(out_dir, "perf_by_hour.csv"), perf_hour, ["hour", "n", "pos_rate", "auc", "brier"])

    # summary
    summary = {
        "ts": int(_now_ms()),
        "data_path": str(args.data_path),
        "meta_json": str(args.meta_json),
        "schema_ver": str(meta.get("ver") or ""),
        "schema_hash": str(meta.get("schema_hash") or ""),
        "n_rows": int(len(df)),
        "n_train": int(len(split.train_idx)),
        "n_val": int(len(split.val_idx)),
        "n_val_used": int(len(y_va)),
        "pos_rate_val": float(y_va.mean()),
        "model": str(args.model),
        "auc_val": float(auc_va),
        "brier_val": float(brier_va),
        "importance_kind": global_imp_kind,
        "n_features": int(len(feature_names)),
        "regime_groups": {"available": regime_keys, "min_group_rows": int(args.min_group_rows)},
        "hour_groups": {"available": hour_keys, "min_group_rows": int(args.min_group_rows)},
        "top_features": _topk(perm_global, k=30),
        "noise_examples": [r["feature"] for r in stab_rows if int(r.get("flag_noise", 0)) == 1][:50],
    }
    with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # human report
    top = _topk(perm_global, k=25)
    noisy = [r for r in stab_rows if int(r.get("flag_noise", 0)) == 1][:25]
    with open(os.path.join(out_dir, "report.md"), "w", encoding="utf-8") as f:
        f.write(f"# Feature selection loop v1\n\n")
        f.write(f"Schema: **{summary['schema_ver']}**  hash={summary['schema_hash'][:16]}…\n\n")
        f.write(f"Model: **{summary['model']}**  AUC(val)={summary['auc_val']:.4f}  Brier(val)={summary['brier_val']:.6f}\n\n")
        f.write("## Top features (perm AUC drop)\n\n")
        f.write("|rank|feature|auc_drop|\n|---:|---|---:|\n")
        for i, (k, v) in enumerate(top, start=1):
            f.write(f"|{i}|{k}|{float(v):.6f}|\n")
        f.write("\n## Noisy candidates (low global + unstable)\n\n")
        f.write("|feature|global_perm_auc_drop|regime_cv|hour_cv|\n|---|---:|---:|---:|\n")
        for r in noisy:
            f.write(
                f"|{r['feature']}|{float(r.get('global_perm_auc_drop', 0.0)):.6f}|"
                f"{float(r.get('regime_cv', 0.0)):.3f}|{float(r.get('hour_cv', 0.0)):.3f}|\n"
            )
        f.write("\n## Perf by regime (val subset)\n\n")
        f.write("|group|n|pos_rate|auc|brier|\n|---|---:|---:|---:|---:|\n")
        for r in perf_regime:
            f.write(
                f"|{r['group']}|{int(r['n'])}|{_fmt_pct(float(r['pos_rate']))}|"
                f"{_fmt_opt(r.get('auc'), '.4f')}|{_fmt_opt(r.get('brier'), '.6f')}|\n"
            )
        f.write("\n## Perf by hour (val subset, UTC)\n\n")
        f.write("|hour|n|pos_rate|auc|brier|\n|---:|---:|---:|---:|---:|\n")
        for r in perf_hour:
            f.write(
                f"|{int(r['hour'])}|{int(r['n'])}|{_fmt_pct(float(r['pos_rate']))}|"
                f"{_fmt_opt(r.get('auc'), '.4f')}|{_fmt_opt(r.get('brier'), '.6f')}|\n"
            )


if __name__ == "__main__":
    main()
