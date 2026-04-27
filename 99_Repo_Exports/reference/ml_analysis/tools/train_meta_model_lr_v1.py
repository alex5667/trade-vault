"""Train MetaModelLR (portable JSON LR) from a dataset parquet.

Train==Serve contract:
  - Feature construction uses the same meta feature builder as runtime
    (build_meta_features_v6/v7/v8).
  - The same per-feature transforms are applied (clip/log1p/etc).
  - Robust scaling is fitted as median/MAD and applied after transforms.
  - LogisticRegression is trained on the scaled features.
  - Time-series CV uses PurgedEmbargoTimeSeriesSplitV2 (purge/embargo by ts_ms).

Dataset expectations:
  - Only flat scalar columns (no nested payload/indicators/blob columns).
  - Indicator fields are stored as scalars, typically with prefix "f_".
  - Base columns: sid, ts_ms, symbol.
  - Labels: y_util_pos_{horizon_ms} (binary), util_r_{horizon_ms}, y_edge_{horizon_ms}, etc.

Usage:
  python -m tools.train_meta_model_lr_v1 \
    --parquet /data/dataset.parquet \
    --schema meta_feat_v8 \
    --horizon-ms 60000 \
    --out /data/meta_lr_v8.json
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import asdict
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score

from core.feature_engineering import RobustScalerPack, apply_transform
from core.meta_model_lr import MetaModelLR
from core.meta_schema_registry import get_schema_info
from core.purged_embargo_split_v2 import PurgedEmbargoTimeSeriesSplitV2


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not np.isfinite(v):
            return float(default)
        return v
    except Exception:
        return float(default)


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)


def _decode_scenario_from_onehot(row: Dict[str, Any]) -> str:
    # e.g. scenario_v4_trend == 1 -> "trend"
    for k, v in row.items():
        if not k.startswith("scenario_v4_"):
            continue
        try:
            if float(v) >= 0.5:
                return k[len("scenario_v4_") :]
        except Exception:
            continue
    return ""


def _get_any(row: Dict[str, Any], keys: List[str], default: Any = 0.0) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return default


def _get_meta_builder(schema_name: str):
    """Return (build_fn, cols, transforms) for schema_name."""
    s = str(schema_name)
    if s == "meta_feat_v8":
        from core.meta_features_v8 import build_meta_features_v8, META_FEAT_V8_COLS, META_FEAT_V8_TRANSFORMS

        return build_meta_features_v8, list(META_FEAT_V8_COLS), dict(META_FEAT_V8_TRANSFORMS)
    if s == "meta_feat_v7":
        from core.meta_features_v7 import build_meta_features_v7, META_FEAT_V7_COLS, META_FEAT_V7_TRANSFORMS

        return build_meta_features_v7, list(META_FEAT_V7_COLS), dict(META_FEAT_V7_TRANSFORMS)
    if s == "meta_feat_v6":
        from core.meta_features_v6 import build_meta_features_v6, META_FEAT_V6_COLS, META_FEAT_V6_TRANSFORMS

        return build_meta_features_v6, list(META_FEAT_V6_COLS), dict(META_FEAT_V6_TRANSFORMS)
    if s == "meta_feat_v9":
        from core.meta_features_v9 import build_meta_features_v9, META_FEAT_V9_COLS, META_FEAT_V9_TRANSFORMS

        return build_meta_features_v9, list(META_FEAT_V9_COLS), dict(META_FEAT_V9_TRANSFORMS)

    raise ValueError(f"unknown_meta_schema: {schema_name}")


def _build_xy_from_df(
    df: pd.DataFrame,
    schema_name: str,
    y_col: str,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, List[str], Dict[str, Any]]:
    """Build X_raw, y, ts_ms, feature_cols, transforms.

    X_raw is built by calling the runtime meta feature builder for each row.
    """

    if y_col not in df.columns:
        raise KeyError(f"missing label column: {y_col}")
    if "ts_ms" not in df.columns:
        raise KeyError("missing required column: ts_ms")

    build_fn, feature_cols, transforms = _get_meta_builder(schema_name)

    # stable, deterministic ordering
    df = df.sort_values("ts_ms", kind="mergesort").reset_index(drop=True)
    rows = df.to_dict(orient="records")

    X = np.zeros((len(rows), len(feature_cols)), dtype=np.float64)

    for i, row in enumerate(rows):
        indicators: Dict[str, Any] = {}
        for k, v in row.items():
            if k.startswith("f_"):
                indicators[k[2:]] = _safe_float(v, 0.0)

        # Common scalars may exist both as standalone columns or inside f_*.
        have = _safe_int(_get_any(row, ["have", "f_have"], indicators.get("have", 0)), 0)
        need = _safe_int(_get_any(row, ["need", "f_need"], indicators.get("need", 0)), 0)
        ok_soft = _safe_int(_get_any(row, ["ok_soft", "f_ok_soft"], indicators.get("ok_soft", 0)), 0)

        rule_score = _safe_float(
            _get_any(row, ["rule_score", "score", "f_rule_score", "f_score"], indicators.get("rule_score", 0.0)),
            0.0,
        )
        exec_risk_norm = _safe_float(
            _get_any(row, ["exec_risk_norm", "f_exec_risk_norm"], indicators.get("exec_risk_norm", 0.0)),
            0.0,
        )
        exec_risk_bps = _safe_float(
            _get_any(row, ["exec_risk_bps", "f_exec_risk_bps"], indicators.get("exec_risk_bps", 0.0)),
            0.0,
        )

        ml_scenario = str(_get_any(row, ["scenario_v4", "f_scenario_v4"], indicators.get("scenario_v4", "")))
        if not ml_scenario:
            ml_scenario = _decode_scenario_from_onehot(row)
        if ml_scenario:
            indicators["scenario_v4"] = ml_scenario

        evidence = {"indicators": indicators}

        feat_out = build_fn(
            evidence=evidence,
            indicators=indicators,
            indicators_with_v4=indicators,
            legs={},
            runtime_snap=None,
            runtime_prev_snap=None,
            have=have,
            need=need,
            ok_soft=ok_soft,
            rule_score=rule_score,
            exec_risk_norm=exec_risk_norm,
            exec_risk_bps=exec_risk_bps,
            ml_scenario=ml_scenario,
        )

        feat = feat_out[0] if isinstance(feat_out, (tuple, list)) and feat_out else feat_out

        for j, name in enumerate(feature_cols):
            X[i, j] = _safe_float(feat.get(name, 0.0) if isinstance(feat, dict) else 0.0, 0.0)

    y = pd.to_numeric(df[y_col], errors="coerce").fillna(0).astype(int).values
    y = np.clip(y, 0, 1).astype(int)

    ts_ms = pd.to_numeric(df["ts_ms"], errors="coerce").fillna(0).astype(np.int64).values
    return X, y, ts_ms, feature_cols, transforms


def _apply_transforms(X_raw: np.ndarray, feature_cols: List[str], transforms: Dict[str, Any]) -> np.ndarray:
    X_tf = np.asarray(X_raw, dtype=np.float64).copy()
    for j, name in enumerate(feature_cols):
        t = transforms.get(name)
        if not t:
            continue
        for i in range(X_tf.shape[0]):
            X_tf[i, j] = float(apply_transform(X_tf[i, j], t))
    return X_tf


def _fit_lr(
    X_scaled: np.ndarray,
    y: np.ndarray,
    C: float,
    max_iter: int,
) -> LogisticRegression:
    lr = LogisticRegression(
        penalty="l2",
        C=float(C),
        class_weight="balanced",
        solver="lbfgs",
        max_iter=int(max_iter),
        n_jobs=1,
    )
    lr.fit(X_scaled, y)
    return lr


def _brier(y_true: np.ndarray, p: np.ndarray) -> float:
    y = y_true.astype(np.float64)
    return float(np.mean((p - y) ** 2))


def train_meta_model_lr_from_df(
    df: pd.DataFrame,
    *,
    schema_name: str,
    y_col: str,
    n_splits: int,
    purge_ms: int,
    embargo_ms: int,
    C: float,
    max_iter: int,
    threshold: float,
) -> Tuple[MetaModelLR, Dict[str, Any]]:
    """Core training routine used by both CLI and tests."""

    X_raw, y, ts_ms, feature_cols, transforms = _build_xy_from_df(df, schema_name=schema_name, y_col=y_col)

    X_tf_full = _apply_transforms(X_raw, feature_cols, transforms)

    pos_rate = float(np.mean(y)) if len(y) else 0.0

    # CV (purged/embargoed): fit scaler+lr per fold to avoid leakage.
    fold_metrics: List[Dict[str, Any]] = []
    if int(n_splits) > 1 and len(y) >= 50:
        splitter = PurgedEmbargoTimeSeriesSplitV2(
            n_splits=int(n_splits),
            purge_ms=int(purge_ms),
            embargo_ms=int(embargo_ms),
        )

        for k, (tr_idx, te_idx) in enumerate(splitter.split(ts_ms), 1):
            tr = np.asarray(tr_idx, dtype=np.int64)
            te = np.asarray(te_idx, dtype=np.int64)
            if len(te) == 0 or len(tr) == 0:
                continue

            rs = RobustScalerPack.fit(X_tf_full[tr], feature_cols)
            X_tr = rs.transform(X_tf_full[tr], feature_cols)
            X_te = rs.transform(X_tf_full[te], feature_cols)

            lr = _fit_lr(X_tr, y[tr], C=C, max_iter=max_iter)
            p_te = lr.predict_proba(X_te)[:, 1]

            try:
                auc = float(roc_auc_score(y[te], p_te))
            except Exception:
                auc = float("nan")
            try:
                ll = float(log_loss(y[te], p_te, labels=[0, 1]))
            except Exception:
                ll = float("nan")

            fold_metrics.append(
                {
                    "fold": int(k),
                    "train_n": int(len(tr)),
                    "test_n": int(len(te)),
                    "auc": auc,
                    "logloss": ll,
                    "brier": _brier(y[te], p_te),
                }
            )

    # Final fit on full dataset
    rs_full = RobustScalerPack.fit(X_tf_full, feature_cols)
    X_scaled = rs_full.transform(X_tf_full, feature_cols)
    lr_full = _fit_lr(X_scaled, y, C=C, max_iter=max_iter)

    p_full = lr_full.predict_proba(X_scaled)[:, 1]
    try:
        auc_full = float(roc_auc_score(y, p_full))
    except Exception:
        auc_full = float("nan")
    try:
        ll_full = float(log_loss(y, p_full, labels=[0, 1]))
    except Exception:
        ll_full = float("nan")

    ver, cols, schema_hash = get_schema_info(str(schema_name))
    cols_hash = MetaModelLR.compute_feature_cols_hash(feature_cols)

    model = MetaModelLR(
        features=list(feature_cols),
        intercept=float(lr_full.intercept_[0]),
        coef=[float(x) for x in lr_full.coef_[0].tolist()],
        threshold=float(threshold),
        schema_name=str(schema_name),
        schema_version=int(ver),
        schema_hash=str(schema_hash),
        feature_cols_hash=str(cols_hash),
        transforms=transforms,
        robust_scaler=rs_full,
    )

    summary: Dict[str, Any] = {
        "y_col": str(y_col),
        "n_rows": int(len(df)),
        "pos_rate": float(pos_rate),
        "cv": fold_metrics,
        "cv_mean": {
            "auc": float(np.nanmean([m["auc"] for m in fold_metrics])) if fold_metrics else None,
            "logloss": float(np.nanmean([m["logloss"] for m in fold_metrics])) if fold_metrics else None,
            "brier": float(np.nanmean([m["brier"] for m in fold_metrics])) if fold_metrics else None,
        },
        "train_full": {"auc": float(auc_full), "logloss": float(ll_full), "brier": _brier(y, p_full)},
        "splits": {"n_splits": int(n_splits), "purge_ms": int(purge_ms), "embargo_ms": int(embargo_ms)},
        "lr": {"C": float(C), "max_iter": int(max_iter), "class_weight": "balanced", "solver": "lbfgs"},
    }

    return model, summary


def main() -> int:
    ap = argparse.ArgumentParser()

    ap.add_argument("--parquet", "--input", dest="parquet", required=True, help="Dataset parquet path")
    ap.add_argument("--out", "--output", dest="out", required=True, help="Output JSON path")

    ap.add_argument("--schema", default="meta_feat_v8", help="Meta schema name")
    ap.add_argument("--horizon-ms", type=int, default=60000, help="Label horizon in ms")
    ap.add_argument("--y-col", default="", help="Override label column (default: y_util_pos_{horizon_ms})")

    ap.add_argument("--n-splits", type=int, default=5, help="CV folds")
    ap.add_argument("--purge-ms", type=int, default=60_000, help="Purging window in ms")
    ap.add_argument("--embargo-ms", type=int, default=30_000, help="Embargo window in ms")

    ap.add_argument("--C", type=float, default=1.0, help="LR inverse regularization")
    ap.add_argument("--max-iter", type=int, default=200, help="LR max_iter")
    ap.add_argument("--threshold", type=float, default=0.5, help="Decision threshold stored in artifact")

    ap.add_argument("--max-rows", type=int, default=0, help="Optional cap for quick experiments")
    ap.add_argument("--outcomes", default="", help="Outcomes NDJSON path (auto-detected if empty)")

    args = ap.parse_args()

    y_col = str(args.y_col or "").strip() or f"y_util_pos_{int(args.horizon_ms)}"

    _path = str(args.parquet)
    if _path.endswith(".ndjson") or _path.endswith(".jsonl"):
        df = pd.read_json(_path, lines=True)
    else:
        df = pd.read_parquet(_path)
    if int(args.max_rows) > 0:
        df = df.head(int(args.max_rows)).copy()

    # ── Auto-join with outcomes + flatten indicators for confirm_train_v7 ndjson ──
    if y_col not in df.columns and "indicators" in df.columns:
        # Determine outcomes path
        outcomes_path = str(args.outcomes or "").strip()
        if not outcomes_path:
            _dir = os.path.dirname(_path)
            outcomes_path = os.path.join(_dir, "latest_outcomes.ndjson")
        if os.path.isfile(outcomes_path):
            print(f"[meta_lr] Auto-joining with outcomes: {outcomes_path}")
            df_out = pd.read_json(outcomes_path, lines=True)
            # Join on sid
            if "sid" in df.columns and "sid" in df_out.columns:
                df_out = df_out.rename(columns={
                    c: f"outcome_{c}" for c in df_out.columns if c != "sid"
                })
                df = df.merge(df_out, on="sid", how="inner")
                print(f"[meta_lr] Joined {len(df)} rows")
            # Compute r_mult and label
            _pnl = pd.to_numeric(df.get("outcome_pnl", 0), errors="coerce").fillna(0.0)
            _risk = pd.to_numeric(df.get("outcome_risk_usd", 0), errors="coerce").fillna(1.0)
            _risk = _risk.replace(0.0, 1.0)
            df["r_mult"] = _pnl / _risk
            df[y_col] = (df["r_mult"] > 0).astype(int)
            print(f"[meta_lr] Computed {y_col}: pos_rate={df[y_col].mean():.4f}")
        else:
            print(f"[meta_lr] WARNING: outcomes file not found at {outcomes_path}")

        # Flatten nested indicators dict into f_* columns
        if "indicators" in df.columns:
            _ind_series = df["indicators"].apply(
                lambda x: x if isinstance(x, dict) else {}
            )
            ind_df = pd.json_normalize(_ind_series)
            for col in ind_df.columns:
                target = f"f_{col}" if not col.startswith("f_") else col
                if target not in df.columns:
                    df[target] = pd.to_numeric(ind_df[col], errors="coerce").fillna(0.0)
            print(f"[meta_lr] Flattened {len(ind_df.columns)} indicator columns")

    if y_col not in df.columns:
        print(f"ERROR: label column not found: {y_col}")
        return 2

    # Train
    model, summary = train_meta_model_lr_from_df(
        df,
        schema_name=str(args.schema),
        y_col=y_col,
        n_splits=int(args.n_splits),
        purge_ms=int(args.purge_ms),
        embargo_ms=int(args.embargo_ms),
        C=float(args.C),
        max_iter=int(args.max_iter),
        threshold=float(args.threshold),
    )

    # Export (signature stable)
    model.dump(str(args.out))

    # Attach training summary (not part of signature)
    try:
        d = json.loads(open(str(args.out), "r", encoding="utf-8").read())
        d["training_summary"] = {
            **summary,
            "parquet": str(args.parquet),
            "created_ms": int(time.time() * 1000),
        }
        with open(str(args.out), "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

    print(f"Saved: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
