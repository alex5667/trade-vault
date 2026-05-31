"""Nightly retrain bundle for v14_of: LR baseline (champion) + edge_stack_v1 (challenger).

Pipeline:
  1. Export `signals:of:inputs` → of_inputs.ndjson
  2. Export `labels:tb`         → labels_tb_live.ndjson
  3. Cost-aware relabel (strict): join inputs+labels by sid, recompute via
     core.triple_barrier.label_path with cost_bps = spread + 2·fees + slippage
  4. Build dataset: extract og_* + v13_of indicators into ML dataset (JSONL)
  5. Train LR baseline → meta_lr_v14_of_baseline_<ts>.json
  6. Train edge_stack_v1 challenger → edge_stack_v14_of_challenger_<ts>.joblib
  7. (Optional) update Redis cfg:ml_confirm:champion / cfg:ml_confirm:challenger
     IF auto_promote=1 AND new model passes acceptance gates.

Designed to run inside `scanner-v14-of-train-timer` container (6h interval).

Env vars:
  REDIS_URL                       redis://redis-worker-1:6379/0
  V14_INPUTS_STREAM               signals:of:inputs
  V14_LABELS_STREAM               labels:tb
  V14_OUT_DIR                     /var/lib/trade/of_reports/models
  V14_INPUTS_MAX_RECORDS          5000
  V14_LABELS_MAX_RECORDS          5000
  V14_FEES_BPS_ONE_SIDE           2.0
  V14_MIN_DATASET_ROWS            500     # below this, skip training (insufficient data)
  V14_PROMOTE_AUTO                0       # 0 = candidate only; 1 = auto-promote to champion/challenger
  V14_PROMOTE_BRIER_MAX           0.20    # gate: new model rejected if Brier > this
  V14_PROMOTE_ECE_MAX             0.10    # gate: new model rejected if ECE > this (challenger only)
  V14_CHAMPION_KEY                cfg:ml_confirm:champion
  V14_CHALLENGER_KEY              cfg:ml_confirm:challenger
  V14_TRAIN_METRICS_KEY           metrics:v14_of_train:last
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("nightly_v14_of_train")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    try:
        return int(_env(key, str(default)))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(_env(key, str(default)))
    except Exception:
        return default


def _run_cmd(cmd: list[str], log_tag: str) -> bool:
    """Run a subprocess command; log stdout/stderr; return True on success."""
    logger.info("[%s] running: %s", log_tag, " ".join(cmd))
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except Exception as e:
        logger.error("[%s] subprocess failed: %s", log_tag, e)
        return False
    if proc.returncode != 0:
        logger.error("[%s] exit=%d stdout=%s stderr=%s", log_tag, proc.returncode,
                     proc.stdout[-2000:], proc.stderr[-2000:])
        return False
    if proc.stdout.strip():
        logger.info("[%s] %s", log_tag, proc.stdout.strip()[-1500:])
    return True


# ---------------------------------------------------------------------------
# Stage 3: cost-aware relabel (inline; uses core.triple_barrier.label_path)
# ---------------------------------------------------------------------------

def stage_relabel(*, inputs_path: Path, labels_path: Path,
                  out_path: Path, fees_bps_one_side: float) -> dict[str, Any] | None:
    """Re-label each labels:tb entry through strict cost-aware label_path.

    Joins by sid with inputs (for spread/slippage). Skips secondary horizon
    records (primary != 1).
    """
    try:
        from core.triple_barrier import BarrierSpec, label_path
    except Exception as e:
        logger.error("relabel: import core.triple_barrier failed: %s", e)
        return None

    def _f(v: Any, d: float = 0.0) -> float:
        try:
            return float(v) if v is not None else d
        except Exception:
            return d

    def _norm_sid(s: str) -> str:
        return s[len("crypto-of:"):] if s.startswith("crypto-of:") else s

    ind_by_sid: dict[str, dict[str, Any]] = {}
    with inputs_path.open() as f:
        for line in f:
            if not line.strip():
                continue
            try:
                o = json.loads(line)
            except Exception:
                continue
            sid = _norm_sid(str(o.get("sid", "") or ""))
            if sid:
                ind_by_sid[sid] = o.get("indicators", {}) or {}
    logger.info("relabel: indexed %d inputs by sid", len(ind_by_sid))

    n_processed = n_no_sid = n_skip_secondary = 0
    out_rows: list[dict[str, Any]] = []

    with labels_path.open() as f:
        for line in f:
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            prim_raw = rec.get("primary", 0)
            if isinstance(prim_raw, dict):
                prim_raw = prim_raw.get("flag", 0)
            try:
                primary = int(prim_raw or 0)
            except Exception:
                primary = 0
            if not primary:
                n_skip_secondary += 1
                continue
            sid = _norm_sid(str(rec.get("sid", "") or ""))
            ind = ind_by_sid.get(sid)
            if ind is None:
                n_no_sid += 1
                continue
            ticks = rec.get("ticks") or []
            path = [(int(t[0]), float(t[1])) for t in ticks if len(t) >= 2]
            if not path:
                continue

            ts0 = int(rec.get("ts_ms") or 0)
            direction = str(rec.get("direction") or "").upper()
            tp_bps = _f(rec.get("tp_bps"))
            sl_bps = _f(rec.get("sl_bps"))
            h_ms = int(rec.get("h_ms") or 180000)
            entry = float(path[0][1])

            spread_bps = _f(ind.get("spread_bps"))
            slip = _f(
                ind.get("expected_slippage_bps",
                        ind.get("max_expected_slippage_bps_eff",
                                ind.get("slippage_bps_est", 0.0)))
            )
            cost_bps = spread_bps + 2.0 * fees_bps_one_side + slip

            res = label_path(
                ts0_ms=ts0, direction=direction, entry_px=entry, path=path,
                spec=BarrierSpec(h_ms=h_ms, tp_bps=tp_bps, sl_bps=sl_bps, cost_bps=cost_bps),
            )

            out_rows.append({
                "sid": sid,
                "symbol": str(rec.get("symbol") or ""),
                "ts_ms": ts0,
                "direction": direction,
                "h_ms": h_ms,
                "tp_bps": tp_bps,
                "sl_bps": sl_bps,
                "tb_outcome": str(res.outcome),
                "tb_hit_ms": int(res.hit_ms),
                "mae_bps": float(res.mae_bps),
                "mfe_bps": float(res.mfe_bps),
                "adverse_proxy": float(res.adverse_proxy),
                "mae_r": 0.0,
                "mfe_r": 0.0,
                "y_edge": int(rec.get("y_edge", 0) or 0),
                "cost_bps": float(res.cost_bps),
                "realized_close_bps": float(res.realized_close_bps),
                "edge_after_cost_bps": float(res.edge_after_cost_bps),
                "y_edge_cost_aware": int(res.y_edge_cost_aware),
            })
            n_processed += 1

    with out_path.open("w") as f:
        for r in out_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    return {
        "processed": n_processed,
        "skipped_no_sid": n_no_sid,
        "skipped_secondary": n_skip_secondary,
        "out_path": str(out_path),
    }


# ---------------------------------------------------------------------------
# Stage 4: build dataset via existing tool
# ---------------------------------------------------------------------------

def stage_build_dataset(*, inputs_path: Path, tb_labels_path: Path,
                        out_path: Path, y_label_col: str = "y_edge_cost_aware") -> dict[str, Any] | None:
    ok = _run_cmd([
        sys.executable, "-m", "tools.build_dataset_from_inputs_outcomes_v4_tb",
        "--inputs", str(inputs_path),
        "--tb-labels", str(tb_labels_path),
        "--out", str(out_path),
        "--y-label-col", y_label_col,
        "--out-format", "jsonl",
    ], log_tag="build_dataset")
    if not ok:
        return None
    summary_path = out_path.with_suffix(out_path.suffix + ".json")
    if not summary_path.exists():
        return None
    try:
        return json.loads(summary_path.read_text())
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Feature set: full feature schema (v15_of by default; v14_of via env-override).
# v15_of = v14_of + ext_payload-derived keys that were silently dropped from
# training when the default was v14_of. Env `V14_FEATURE_SCHEMA_VER=v14_of`
# restores the old schema for comparison/rollback.
# Counts are sourced live from core.ml_feature_schema_v{14,15}_of so totals
# never drift in this file. The authoritative v15_of count is pinned by
# ``core.ml_feature_schema_v15_of._EXPECTED_KEYS``.
# ---------------------------------------------------------------------------

# Schema version actually trained. Default 'v15_of' (full ext_payload coverage,
# count pinned by core.ml_feature_schema_v15_of._EXPECTED_KEYS). Override
# `V14_FEATURE_SCHEMA_VER=v14_of` to fall back to the smaller v14_of schema.
# Any other value falls back to v15_of with a warning.
_FEATURE_SCHEMA_VER: str = (os.environ.get("V14_FEATURE_SCHEMA_VER") or "v15_of").strip() or "v15_of"


def _get_feature_cols() -> list[str]:
    if _FEATURE_SCHEMA_VER == "v14_of":
        from core.ml_feature_schema_v14_of import get_v14_of_numeric_keys
        return get_v14_of_numeric_keys()
    if _FEATURE_SCHEMA_VER != "v15_of":
        logger.warning(
            "V14_FEATURE_SCHEMA_VER=%s unrecognized — falling back to v15_of",
            _FEATURE_SCHEMA_VER,
        )
    from core.ml_feature_schema_v15_of import get_v15_of_numeric_keys
    return get_v15_of_numeric_keys()


def _get_canonical_schema_hash() -> str:
    """Canonical schema_hash for the active schema, matching the Feature Registry
    contract-check pin (cfg:feature_registry:edge_stack:<schema_ver>). Computed
    from the live feature_registry so the trained artifact stays aligned across
    schema bumps without manual label edits."""
    try:
        from core.feature_registry import get_schema_info
        return get_schema_info(_FEATURE_SCHEMA_VER).schema_hash
    except Exception:
        return ""


def _feature_schema_version_int() -> int:
    if _FEATURE_SCHEMA_VER == "v14_of":
        return 14
    return 15


def _schema_label(role: str) -> str:
    """Generate schema_name label for serialized models, e.g.
    'v14_of_baseline_lr' / 'v15_of_baseline_stack'."""
    return f"{_FEATURE_SCHEMA_VER}_baseline_{role}"


def _run_id(role: str, ts_str: str) -> str:
    """Generate run_id with the active schema_ver baked in."""
    if role == "lr":
        return f"{_FEATURE_SCHEMA_VER}_baseline_{ts_str}"
    return f"edge_stack_{_FEATURE_SCHEMA_VER}_challenger_{ts_str}"


def _model_filename(role: str, ts_str: str) -> str:
    if role == "lr":
        return f"meta_lr_{_FEATURE_SCHEMA_VER}_baseline_{ts_str}.json"
    return f"edge_stack_{_FEATURE_SCHEMA_VER}_challenger_{ts_str}.joblib"


# Module-level cache — loaded once per process.
V14_BASE_FEATURES: list[str] = _get_feature_cols()


def _load_dataset(path: Path) -> tuple[Any, Any, Any]:
    """Return (X, y, ts_arr). ts_arr is int64 epoch-ms from row-level ts_ms."""
    import numpy as np

    def _ff(v: Any, d: float = 0.0) -> float:
        try:
            if v is None:
                return d
            if isinstance(v, bool):
                return 1.0 if v else 0.0
            return float(v)
        except Exception:
            return d

    rows: list[dict[str, Any]] = []
    with path.open() as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    X = np.array([
        [_ff((r.get("indicators", {}) or {}).get(k)) for k in V14_BASE_FEATURES]
        for r in rows
    ], dtype=np.float64)
    y = np.array([int(r.get("y_edge", 0) or 0) for r in rows], dtype=np.int64)
    ts_arr = np.array([int(r.get("ts_ms") or 0) for r in rows], dtype=np.int64)
    return X, y, ts_arr


# ---------------------------------------------------------------------------
# Stage 5: train LR baseline → MetaModelLR JSON
# ---------------------------------------------------------------------------

def stage_train_lr(*, dataset_path: Path, out_dir: Path, ts_str: str,
                   holdout_hours: int = 0) -> dict[str, Any] | None:
    """Train sklearn LR on v14_of dataset, write MetaModelLR-compatible JSON.

    holdout_hours > 0: carves the last N hours as a temporal hold-out for an
    out-of-time quality gate (does NOT affect the final production model, which
    is always fit on all data).
    """
    try:
        import numpy as np
        import statistics
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import (average_precision_score, brier_score_loss,
                                     log_loss, roc_auc_score)
        from sklearn.model_selection import StratifiedKFold
        from sklearn.preprocessing import StandardScaler
    except Exception as e:
        logger.error("train_lr: sklearn import failed: %s", e)
        return None

    X, y, ts_arr = _load_dataset(dataset_path)
    if len(y) < 100 or int(y.sum()) < 10:
        logger.error("train_lr: insufficient data n=%d pos=%d", len(y), int(y.sum()))
        return None

    # ── profit_factor_roll20 drift snapshot ──────────────────────────────────
    pf20_stats: dict[str, Any] = {}
    if "profit_factor_roll20" in V14_BASE_FEATURES:
        pf20_idx = V14_BASE_FEATURES.index("profit_factor_roll20")
        pf20_col: Any = X[:, pf20_idx]
        valid_mask: Any = ~np.isnan(pf20_col) & (pf20_col > 0)
        valid_vals: Any = pf20_col[valid_mask]
        if len(valid_vals) > 0:
            pf20_stats = {
                "median": float(np.median(valid_vals)),
                "p25": float(np.percentile(valid_vals, 25)),
                "p75": float(np.percentile(valid_vals, 75)),
                "n_valid": int(len(valid_vals)),
                "n_total": int(len(pf20_col)),
            }
            logger.info("profit_factor_roll20: median=%.3f p25=%.3f p75=%.3f n=%d/%d",
                        pf20_stats["median"], pf20_stats["p25"], pf20_stats["p75"],
                        pf20_stats["n_valid"], pf20_stats["n_total"])

    # ── Temporal hold-out split (last holdout_hours as out-of-time test) ─────
    # CV and hold-out evaluation use X_train/y_train; final production model
    # uses ALL data (X, y) for best generalization.
    X_train: Any = X
    y_train: Any = y
    X_ho: Any = None
    y_ho: Any = None
    holdout_metrics: dict[str, Any] = {}

    if holdout_hours > 0 and len(ts_arr) > 0 and int(ts_arr.max()) > 0:
        ts_max = int(ts_arr.max())
        cutoff = ts_max - holdout_hours * 3_600_000
        train_mask: Any = ts_arr <= cutoff
        ho_mask: Any = ts_arr > cutoff
        n_tr = int(train_mask.sum())
        n_ho = int(ho_mask.sum())
        if n_tr >= 100 and n_ho >= 20:
            X_train, y_train = X[train_mask], y[train_mask]
            X_ho, y_ho = X[ho_mask], y[ho_mask]
            holdout_metrics = {"n_rows": n_ho, "n_pos": int(y_ho.sum())}
            logger.info("holdout split: train_n=%d holdout_n=%d holdout_pos=%d",
                        n_tr, n_ho, int(y_ho.sum()))
        else:
            logger.warning("holdout split insufficient (train=%d holdout=%d) — using all data", n_tr, n_ho)

    # ── 5-fold CV on training portion ────────────────────────────────────────
    auc_l: list[float] = []
    ap_l: list[float] = []
    brier_l: list[float] = []
    ll_l: list[float] = []
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    pass_at_pmin_l: list[float] = []
    for tr, te in skf.split(X_train, y_train):
        sc_cv = StandardScaler().fit(X_train[tr])
        Xtr: Any = sc_cv.transform(X_train[tr])
        Xte: Any = sc_cv.transform(X_train[te])
        lr_cv = LogisticRegression(C=1.0, max_iter=2000, class_weight="balanced", random_state=42)
        lr_cv.fit(Xtr, y_train[tr])
        p = lr_cv.predict_proba(Xte)[:, 1]
        if len(set(y_train[te])) > 1:
            auc_l.append(roc_auc_score(y_train[te], p))  # type: ignore
            ap_l.append(average_precision_score(y_train[te], p))  # type: ignore
        brier_l.append(brier_score_loss(y_train[te], p))  # type: ignore
        ll_l.append(log_loss(y_train[te], p, labels=[0, 1]))  # type: ignore
        # fraction of OOF predictions that reach p_min threshold
        pass_at_pmin_l.append(float((p >= 0.5).mean()))

    cv_metrics = {
        "roc_auc_mean": statistics.mean(auc_l) if auc_l else float("nan"),
        "pr_auc_mean": statistics.mean(ap_l) if ap_l else float("nan"),
        "brier_mean": statistics.mean(brier_l),
        "log_loss_mean": statistics.mean(ll_l),
        "n_rows": int(len(y)),
        "n_train_rows": int(len(y_train)),
        "pos_rate": float(y.mean()),  # type: ignore
        # What fraction of OOF preds cross p_min=0.5? <2% → model is uncalibrated/biased.
        "pass_rate_at_p_min": statistics.mean(pass_at_pmin_l) if pass_at_pmin_l else 0.0,
    }

    # ── Temporal hold-out evaluation ─────────────────────────────────────────
    if X_ho is not None and holdout_metrics.get("n_rows", 0) >= 20 and holdout_metrics.get("n_pos", 0) >= 2:
        sc_ho = StandardScaler().fit(X_train)
        lr_ho = LogisticRegression(C=1.0, max_iter=2000, class_weight="balanced", random_state=42)
        lr_ho.fit(sc_ho.transform(X_train), y_train)
        p_ho: Any = lr_ho.predict_proba(sc_ho.transform(X_ho))[:, 1]
        if len(set(y_ho.tolist())) > 1:
            holdout_metrics["roc_auc"] = float(roc_auc_score(y_ho, p_ho))
            holdout_metrics["brier"] = float(brier_score_loss(y_ho, p_ho))
            logger.info("holdout eval: AUC=%.4f brier=%.4f n=%d pos=%d",
                        holdout_metrics["roc_auc"], holdout_metrics["brier"],
                        holdout_metrics["n_rows"], holdout_metrics["n_pos"])
        else:
            holdout_metrics["roc_auc"] = 0.0
            logger.warning("holdout eval: only one class — AUC skipped")

    # ── Final model on ALL data (X, y) for best production generalization ────
    scaler = StandardScaler().fit(X)
    lr = LogisticRegression(C=1.0, max_iter=2000, class_weight="balanced", random_state=42)
    lr.fit(scaler.transform(X), y)

    assert scaler.mean_ is not None
    assert scaler.scale_ is not None
    mean_arr: Any = scaler.mean_
    scale_arr: Any = scaler.scale_
    robust_scaler_params = {
        feat: {
            "center": float(mean_arr[i]),
            "scale": float(scale_arr[i] if scale_arr[i] > 1e-9 else 1.0),
        }
        for i, feat in enumerate(V14_BASE_FEATURES)
    }

    intercept_arr: Any = lr.intercept_
    coef_arr: Any = lr.coef_
    pack = {
        "features": list(V14_BASE_FEATURES),
        "intercept": float(intercept_arr[0]),
        "coef": [float(c) for c in coef_arr[0]],
        "threshold": 0.5,
        "transforms": {},
        "robust_scaler": robust_scaler_params,
        "schema_name": _schema_label("lr"),
        # Model artifact's schema_version IS the feature_schema_version
        # (MetaModelLR.load reads schema_version OR feature_schema_version).
        # Distinct from the cfg envelope's schema_version=1 in stage_publish
        # below — that one is the Redis cfg shape version validated by
        # services/ml_confirm/champion_cfg.py.
        "schema_version": _feature_schema_version_int(),
        "schema_hash": _get_canonical_schema_hash(),
        "feature_cols_hash": hashlib.sha256(",".join(V14_BASE_FEATURES).encode("utf-8")).hexdigest()[:16],
        "created_ms": int(time.time() * 1000),
        "model_signature": "",
        "metrics": cv_metrics,
        "kind": "meta_lr",
        "run_id": _run_id("lr", ts_str),
        "feature_schema_ver": _FEATURE_SCHEMA_VER,
        "feature_schema_version": _feature_schema_version_int(),
    }
    sig_in = dict(pack)
    sig_in.pop("model_signature", None)
    pack["model_signature"] = hashlib.sha256(
        json.dumps(sig_in, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:16]

    out_path = out_dir / _model_filename("lr", ts_str)
    with out_path.open("w") as f:
        json.dump(pack, f, indent=2, ensure_ascii=False)
    logger.info("train_lr: wrote %s (cv_metrics=%s)", out_path, cv_metrics)

    return {
        "path": str(out_path),
        "run_id": pack["run_id"],
        "signature": pack["model_signature"],
        "metrics": cv_metrics,
        "holdout_metrics": holdout_metrics,
        "pf20_stats": pf20_stats,
    }


# ---------------------------------------------------------------------------
# Stage 6: train edge_stack_v1 challenger → joblib
# ---------------------------------------------------------------------------

def stage_train_gbdt(*, dataset_path: Path, out_dir: Path, ts_str: str) -> dict[str, Any] | None:
    """Train edge_stack_v1: sklearn LR pipeline + GBDT + meta-LR on OOF."""
    try:
        import joblib
        import numpy as np
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import (average_precision_score, brier_score_loss,
                                     log_loss, roc_auc_score)
        from sklearn.model_selection import StratifiedKFold
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
    except Exception as e:
        logger.error("train_gbdt: sklearn/joblib import failed: %s", e)
        return None

    X, y, _ts_gbdt = _load_dataset(dataset_path)
    if len(y) < 200 or int(y.sum()) < 15:
        logger.error("train_gbdt: insufficient data n=%d pos=%d", len(y), int(y.sum()))
        return None

    # OOF predictions to train meta on unbiased base outputs.
    oof_p_lr = np.zeros(len(y), dtype=np.float64)
    oof_p_gbdt = np.zeros(len(y), dtype=np.float64)
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    for tr, te in skf.split(X, y):
        sc = StandardScaler().fit(X[tr])
        Xtr: Any = sc.transform(X[tr])
        Xte: Any = sc.transform(X[te])
        lr = LogisticRegression(C=1.0, max_iter=2000, class_weight="balanced", random_state=42)
        lr.fit(Xtr, y[tr])
        oof_p_lr[te] = lr.predict_proba(Xte)[:, 1]
        gbdt = GradientBoostingClassifier(
            n_estimators=200, max_depth=3, learning_rate=0.05,
            min_samples_leaf=20, subsample=0.8, random_state=42,
        )
        gbdt.fit(X[tr], y[tr])
        oof_p_gbdt[te] = gbdt.predict_proba(X[te])[:, 1]

    # Final base models (trained on ALL)
    lr_pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("lr", LogisticRegression(C=1.0, max_iter=2000, class_weight="balanced", random_state=42)),
    ])
    lr_pipeline.fit(X, y)

    gbdt_full = GradientBoostingClassifier(
        n_estimators=200, max_depth=3, learning_rate=0.05,
        min_samples_leaf=20, subsample=0.8, random_state=42,
    )
    gbdt_full.fit(X, y)

    # Meta LR on OOF
    Z: Any = np.column_stack([oof_p_lr, oof_p_gbdt])
    meta_lr_model = LogisticRegression(C=1.0, max_iter=2000, class_weight="balanced", random_state=42)
    meta_lr_model.fit(Z, y)

    # OOF stack predictions for evaluation
    p_meta_oof = meta_lr_model.predict_proba(Z)[:, 1]
    metrics = {
        "roc_auc_oof": roc_auc_score(y, p_meta_oof),  # type: ignore
        "pr_auc_oof": average_precision_score(y, p_meta_oof),  # type: ignore
        "brier_oof": brier_score_loss(y, p_meta_oof),  # type: ignore
        "log_loss_oof": log_loss(y, p_meta_oof, labels=[0, 1]),  # type: ignore
        "n_rows": len(y),
        "pos_rate": y.mean(),  # type: ignore
    }

    feature_cols_hash = hashlib.md5(",".join(V14_BASE_FEATURES).encode("utf-8")).hexdigest()
    pack = {
        "kind": "edge_stack_v1",
        "lr": lr_pipeline,
        "gbdt": gbdt_full,
        "meta": meta_lr_model,
        "feature_cols": list(V14_BASE_FEATURES),
        "feature_cols_hash": feature_cols_hash,
        "n_features_expected": len(V14_BASE_FEATURES),
        "feature_schema_version": _feature_schema_version_int(),
        "feature_schema_ver": _FEATURE_SCHEMA_VER,
        "schema_name": _schema_label("stack"),
        "created_ms": int(time.time() * 1000),
        "run_id": _run_id("stack", ts_str),
        "metrics": metrics,
    }

    out_path = out_dir / _model_filename("stack", ts_str)
    joblib.dump(pack, out_path)
    logger.info("train_gbdt: wrote %s (metrics=%s)", out_path, metrics)

    return {
        "path": str(out_path),
        "run_id": pack["run_id"],
        "feature_cols_hash": feature_cols_hash,
        "metrics": metrics,
    }


# ---------------------------------------------------------------------------
# Stage 7: publish Redis cfg (candidate keys always; champion/challenger only if auto_promote)
# ---------------------------------------------------------------------------

def stage_publish(*, redis_url: str, lr_info: dict | None, gbdt_info: dict | None,
                  champion_key: str, challenger_key: str,
                  auto_promote: bool,
                  promote_brier_max: float, promote_ece_max: float,
                  promote_holdout_min_auc: float = 0.0) -> dict[str, Any]:
    """Always writes candidate keys; updates champion/challenger only if gates pass."""
    try:
        import redis
        r: Any = redis.Redis.from_url(redis_url, decode_responses=True)
    except Exception as e:
        logger.error("publish: redis connect failed: %s", e)
        return {"published": False, "error": str(e)}

    result: dict[str, Any] = {"candidate_keys": [], "promoted": []}

    # Always write candidate cfg (safe — separate keys, not consumed by inference)
    if lr_info:
        cand_lr = {
            "schema_version": 1,
            "kind": "meta_lr",
            "run_id": lr_info["run_id"],
            "created_ms": int(time.time() * 1000),
            "model_path": lr_info["path"],
            "mode": "SHADOW",
            "enforce_share": 0.0,
            "p_min": 0.5,
            "feature_schema_ver": _FEATURE_SCHEMA_VER,
            "fail_policy": "OPEN",
            "model_signature": lr_info.get("signature", ""),
            "metrics": lr_info.get("metrics", {}),
        }
        _lr_cand_key = f"cfg:ml_confirm:{_FEATURE_SCHEMA_VER}:lr_candidate"
        r.set(_lr_cand_key, json.dumps(cand_lr, separators=(",", ":")))
        result["candidate_keys"].append(_lr_cand_key)

    if gbdt_info:
        cand_gbdt = {
            "schema_version": 1,
            "kind": "edge_stack_v1",
            "run_id": gbdt_info["run_id"],
            "created_ms": int(time.time() * 1000),
            "model_path": gbdt_info["path"],
            "mode": "SHADOW",
            "enforce_share": 0.0,
            "p_min": 0.5,
            "feature_schema_ver": _FEATURE_SCHEMA_VER,
            "fail_policy": "OPEN",
            "metrics": gbdt_info.get("metrics", {}),
        }
        _gbdt_cand_key = f"cfg:ml_confirm:{_FEATURE_SCHEMA_VER}:gbdt_candidate"
        r.set(_gbdt_cand_key, json.dumps(cand_gbdt, separators=(",", ":")))
        result["candidate_keys"].append(_gbdt_cand_key)

    if not auto_promote:
        result["promoted"] = []
        result["promote_reason"] = "auto_promote_disabled"
        return result

    def _v15_blocks_promote(*, new_auc: float) -> tuple[bool, str]:
        """Block v14_of from overwriting champion if a v15_of model is already there with better AUC.
        v15_of trains less frequently; guard prevents regression when v14-of-train-timer cycles."""
        if _FEATURE_SCHEMA_VER == "v15_of":
            return False, "training_v15_itself"
        try:
            raw = r.get(champion_key)
            if not raw:
                return False, "no_current_champion"
            cur = json.loads(str(raw))
            if cur.get("feature_schema_ver") != "v15_of":
                return False, "current_not_v15"
            cur_m = cur.get("metrics") or {}
            cur_auc = float(cur_m.get("roc_auc_mean") or cur_m.get("roc_auc_oof") or 0.0)
            min_delta = 0.005
            if cur_auc > new_auc + min_delta:
                return True, f"v15_champion_auc={cur_auc:.4f}>v14_auc={new_auc:.4f}+{min_delta}"
        except Exception as exc:
            logger.warning("_v15_blocks_promote check failed: %s — allowing promote", exc)
        return False, "ok"

    # Promotion gates
    def _pass_lr() -> tuple[bool, str]:
        if not lr_info:
            return False, "no_lr_info"
        m = lr_info.get("metrics", {})
        br = float(m.get("brier_mean", 1.0))
        if br > promote_brier_max:
            return False, f"brier_too_high({br:.4f}>{promote_brier_max:.4f})"
        if promote_holdout_min_auc > 0.0:
            ho = lr_info.get("holdout_metrics") or {}
            ho_auc = float(ho.get("roc_auc") or 0.0)
            if ho_auc > 0.0 and ho_auc < promote_holdout_min_auc:
                return False, f"holdout_auc_too_low({ho_auc:.4f}<{promote_holdout_min_auc:.4f})"
        # Threshold-reachability gate: if <2% of OOF predictions reach p_min=0.5,
        # the model is too biased to produce actionable signals → block.
        pass_rate = float(m.get("pass_rate_at_p_min", 1.0))
        min_pass_rate = float(os.environ.get("V14_PROMOTE_MIN_PASS_RATE", "0.02"))
        if pass_rate < min_pass_rate:
            return False, f"pass_rate_at_p_min_too_low({pass_rate:.3f}<{min_pass_rate:.3f})"
        return True, "ok"

    def _pass_gbdt() -> tuple[bool, str]:
        if not gbdt_info:
            return False, "no_gbdt_info"
        m = gbdt_info.get("metrics", {})
        br = float(m.get("brier_oof", 1.0))
        if br > promote_brier_max:
            return False, f"brier_too_high({br:.4f}>{promote_brier_max:.4f})"
        return True, "ok"

    if lr_info:
        ok, reason = _pass_lr()
        if ok:
            new_auc = float((lr_info.get("metrics") or {}).get("roc_auc_mean") or 0.0)
            blocked, block_reason = _v15_blocks_promote(new_auc=new_auc)
            if blocked:
                logger.info("promote LR: skipped — %s", block_reason)
                result["promote_skip_lr"] = block_reason
                ok = False
        if ok:
            # backup previous + promote
            try:
                prev = r.get(champion_key)
                if prev:
                    r.set(champion_key + "_prev_nightly", str(prev))
            except Exception:
                pass
            cfg = {
                "schema_version": 1,
                "kind": "meta_lr",
                "run_id": lr_info["run_id"],
                "created_ms": int(time.time() * 1000),
                "model_path": lr_info["path"],
                "mode": "SHADOW",   # auto-promote keeps SHADOW; human flips to ENFORCE
                "enforce_share": 0.10,  # 10% canary — matches ML_CONFIRM_ENFORCE_SHARE env
                "p_min": 0.5,
                "feature_schema_ver": _FEATURE_SCHEMA_VER,
                "fail_policy": "OPEN",
                "model_signature": lr_info.get("signature", ""),
                # class_weight="balanced" corrects the bias term so raw probabilities
                # span the full [0,1] range. The isotonic sibling calibrator (autopilot)
                # will refit within minutes on the new champion's outputs.
                "calibrate_p_edge": True,
                # util_floors: effective p_min for _decide_meta_lr; 0.27 matches the
                # isotonic calibrator's output range while the calibrator accumulates data
                # from the balanced model. Raise once calibration stabilises (≥ 7d data).
                "util_floors": {
                    "global": {"floor": 0.27},
                    "by_bucket": {
                        "trend": {"floor": 0.28},
                        "range": {"floor": 0.27},
                        "other": {"floor": 0.25},
                    },
                },
            }
            r.set(champion_key, json.dumps(cfg, separators=(",", ":")))
            result["promoted"].append({"key": champion_key, "kind": "meta_lr"})
        else:
            result["promote_skip_lr"] = reason

    if gbdt_info:
        ok, reason = _pass_gbdt()
        if ok:
            try:
                prev = r.get(challenger_key)
                if prev:
                    r.set(challenger_key + "_prev_nightly", str(prev))
            except Exception:
                pass
            cfg = {
                "schema_version": 1,
                "kind": "edge_stack_v1",
                "run_id": gbdt_info["run_id"],
                "created_ms": int(time.time() * 1000),
                "model_path": gbdt_info["path"],
                "mode": "SHADOW",
                "enforce_share": 0.0,
                "p_min": 0.5,
                "feature_schema_ver": _FEATURE_SCHEMA_VER,
                "fail_policy": "OPEN",
                "calibrate_p_edge": True,  # class_weight="balanced" fixes bias; sibling calibrator refits on autopilot
            }
            r.set(challenger_key, json.dumps(cfg, separators=(",", ":")))
            result["promoted"].append({"key": challenger_key, "kind": "edge_stack_v1"})
        else:
            result["promote_skip_gbdt"] = reason

    return result


# ---------------------------------------------------------------------------
# Stage 1+2: export streams to NDJSON
# ---------------------------------------------------------------------------

def _scan_primary_label_ts_range(labels_path: Path) -> tuple[int, int] | None:
    """Return (min_ts_ms, max_ts_ms) over primary=1 labels in NDJSON file.

    Used to bound the inputs export so we always fetch inputs that overlap the
    label window (the join key is sid which embeds ts_ms). The export tool
    reads XRANGE forward from a start_id; without an explicit time-bound the
    `--max-records` cap returns OLDEST records in the window and inputs never
    overlap with the (typically more recent) primary labels — that was the
    root cause of `relabel processed=0`.
    """
    min_ts: int | None = None
    max_ts: int | None = None
    n_primary = 0
    with labels_path.open() as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                o = json.loads(s)
            except Exception:
                continue
            prim_raw = o.get("primary", 0)
            if isinstance(prim_raw, dict):
                prim_raw = prim_raw.get("flag", 0)
            try:
                primary = int(prim_raw or 0)
            except Exception:
                primary = 0
            if not primary:
                continue
            try:
                ts = int(o.get("ts_ms") or 0)
            except Exception:
                ts = 0
            if ts <= 0:
                continue
            n_primary += 1
            if min_ts is None or ts < min_ts:
                min_ts = ts
            if max_ts is None or ts > max_ts:
                max_ts = ts
    if min_ts is None or max_ts is None:
        return None
    logger.info("label_ts_range: primary=%d min_ts_ms=%d max_ts_ms=%d span_h=%.1f",
                n_primary, min_ts, max_ts, (max_ts - min_ts) / 3600_000.0)
    return (min_ts, max_ts)


def stage_export(*, redis_url: str, inputs_stream: str, labels_stream: str,
                 inputs_max: int, labels_max: int, labels_since_hours: int,
                 work_dir: Path) -> dict[str, Path] | None:
    inputs_path = work_dir / "of_inputs.ndjson"
    labels_path = work_dir / "labels_tb_live.ndjson"

    # 1) Export labels first so we can derive the time window that inputs must
    # cover. `labels:tb` records are ~4KB so even ~50k fit comfortably.
    ok2 = _run_cmd([
        sys.executable, "-m", "tools.export_stream_payload_ndjson_v1",
        "--redis-url", redis_url,
        "--stream", labels_stream,
        "--payload-field", "payload",
        "--out", str(labels_path),
        "--since-hours", str(labels_since_hours),
        "--max-scan", str(labels_max),
    ], log_tag="export_labels")

    if not (ok2 and labels_path.exists()):
        return None

    # 2) Derive bound from primary=1 labels and export inputs covering it.
    # `signals:of:inputs` records are ~40KB so we MUST bound by ts_ms — bumping
    # `--max-records` blindly to cover 72h would push the NDJSON above 1GB.
    inputs_cmd = [
        sys.executable, "-m", "tools.export_of_inputs_ndjson_v2",
        "--redis-url", redis_url,
        "--stream", inputs_stream,
        "--out", str(inputs_path),
        "--max-records", str(inputs_max),
    ]
    ts_range = _scan_primary_label_ts_range(labels_path)
    if ts_range is not None:
        # 5-min back-buffer covers minor publisher clock skew vs label ts_ms.
        since_ms = max(0, ts_range[0] - 5 * 60_000)
        inputs_cmd.extend(["--since-ts-ms", str(since_ms)])
        logger.info("inputs export bounded by labels: since_ts_ms=%d", since_ms)
    else:
        # Fallback if labels file has no primary=1 records.
        inputs_cmd.extend(["--since-hours", str(labels_since_hours)])
        logger.warning("no primary=1 labels found; falling back to since-hours=%d", labels_since_hours)

    ok1 = _run_cmd(inputs_cmd, log_tag="export_inputs")

    if not (ok1 and inputs_path.exists()):
        return None

    return {"inputs": inputs_path, "labels": labels_path}


# ---------------------------------------------------------------------------
# Train report: build text + send to notify:telegram
# ---------------------------------------------------------------------------

def _build_train_report_text(
    *,
    metrics: dict[str, Any],
    dataset_path: Path,
    tb_path: Path,
    status: str,
    elapsed_sec: float,
) -> str:
    """Build a plain-text Telegram report with per-metric explanations."""
    try:
        import numpy as np
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import brier_score_loss, roc_auc_score
        from sklearn.model_selection import StratifiedKFold
        from sklearn.preprocessing import StandardScaler
    except ImportError:
        return ""

    # ── load dataset ──────────────────────────────────────────────────────────
    rows: list[dict[str, Any]] = []
    if dataset_path.exists():
        with dataset_path.open() as f:
            for line in f:
                s = line.strip()
                if s:
                    try:
                        rows.append(json.loads(s))
                    except Exception:
                        pass
    if not rows:
        return ""

    tb_rows: list[dict[str, Any]] = []
    if tb_path.exists():
        with tb_path.open() as f:
            for line in f:
                s = line.strip()
                if s:
                    try:
                        tb_rows.append(json.loads(s))
                    except Exception:
                        pass

    n = len(rows)
    pos = sum(int(r.get("y_edge", 0) or 0) for r in rows)
    pos_rate = pos / n if n else 0.0

    # ── feature matrix ────────────────────────────────────────────────────────
    feat_cols = list(V14_BASE_FEATURES)

    def _fv(v: Any) -> float:
        try:
            return float(v) if v is not None else 0.0
        except Exception:
            return 0.0

    X = np.array([[_fv((r.get("indicators") or {}).get(k)) for k in feat_cols] for r in rows], dtype=np.float64)
    y = np.array([int(r.get("y_edge", 0) or 0) for r in rows], dtype=np.int64)

    col_med = np.nanmedian(X, axis=0)
    for j in range(X.shape[1]):
        nans = np.isnan(X[:, j])
        X[nans, j] = col_med[j]

    lines: list[str] = []
    ts_label = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())

    lines.append(f"🧠 v14_of Train Report — {ts_label}")
    lines.append(f"Schema: {_FEATURE_SCHEMA_VER} ({len(feat_cols)} фич)  время: {elapsed_sec:.0f}s")
    lines.append("")

    # ── 5.1 Dataset integrity ─────────────────────────────────────────────────
    lines.append("── 5.1 Датасет ──")
    lines.append(f"Rows: {n:,}  Pos: {pos}  Rate: {pos_rate:.2%}")
    # Rate — доля прибыльных сделок (positive class). Определяет сложность задачи
    # и ожидаемый Brier baseline = Rate*(1-Rate). Типично 3–15% для trading.
    brier_baseline = pos_rate * (1.0 - pos_rate)
    lines.append(f"  Rate = доля прибыльных сделок; Brier baseline={brier_baseline:.4f}")

    from collections import Counter as _Counter
    sid_dup = {s: c for s, c in _Counter(r.get("sid", "") for r in rows).items() if c > 1 and s}
    lines.append("✓ Дублей sid нет" if not sid_dup else f"⚠ Дубли sid: {len(sid_dup)} (один сигнал обучает дважды)")

    if tb_rows:
        def _norm(s: str) -> str:
            return s[len("crypto-of:"):] if s.startswith("crypto-of:") else s

        tb_dup = {s: c for s, c in _Counter(_norm(r.get("sid", "") or "") for r in tb_rows).items() if c > 1 and s}
        lines.append("✓ Дублей меток нет" if not tb_dup else f"⚠ Дубли меток: {len(tb_dup)} (смещение в y)")

        hits = sorted(_fv(r.get("tb_hit_ms", 0)) for r in tb_rows if _fv(r.get("tb_hit_ms", 0)) > 0)
        if hits:
            leakage = sum(1 for r in tb_rows if _fv(r.get("tb_hit_ms", 0)) < 0)
            n_h = len(hits)
            p50 = hits[int(n_h * 0.50)]
            p99 = hits[min(int(n_h * 0.99), n_h - 1)]
            if leakage:
                # tb_hit_ms < 0 означает: метка (исход сделки) сформирована ДО входа.
                # Модель видит будущее → переобучение → AUC на prod будет ~0.5.
                lines.append(f"✗ LEAKAGE: {leakage} меток с tb_hit_ms<0 — МОДЕЛЬ ВИДИТ БУДУЩЕЕ!")
            else:
                # tb_hit_ms = время от входа до исхода (TP/SL/timeout).
                # p50/p99 показывают горизонт: p99>h_ms — нормально (timeout).
                lines.append(f"✓ Утечки нет  lag p50={p50/1000:.0f}s p99={p99/1000:.0f}s")
                lines.append(f"  lag = вход→исход (tb_hit_ms); все ≥0 означает нет leakage")
    lines.append("")

    # ── 5.2–5.4: single OOF CV pass ──────────────────────────────────────────
    if n < 100 or pos < 10 or len(set(y.tolist())) < 2:
        lines.append("⚠ Мало данных для CV — пропущено")
        _append_train_metrics(lines, metrics)
        _append_status_line(lines, status, metrics)
        return "\n".join(lines)

    oof_p = np.zeros(n, dtype=np.float64)
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    fold_stats: list[dict[str, Any]] = []

    for fold_i, (tr, te) in enumerate(skf.split(X, y.tolist())):
        sc = StandardScaler().fit(X[tr])
        lr = LogisticRegression(C=1.0, max_iter=2000, random_state=42)
        lr.fit(sc.transform(X[tr]), y[tr])
        p = lr.predict_proba(sc.transform(X[te]))[:, 1]
        oof_p[te] = p
        n_pos_te = int(y[te].sum())
        if n_pos_te >= 2 and len(set(y[te].tolist())) > 1:
            fold_stats.append({
                "fold": fold_i, "n": len(te), "pos": n_pos_te,
                "auc": float(roc_auc_score(y[te], p)),
                "brier": float(brier_score_loss(y[te], p)),
            })

    # 5.2 per-fold table
    lines.append("── 5.2 Кросс-валидация (5 фолдов) ──")
    # AUC (ROC-AUC): способность модели ранжировать — отделить прибыльные от убыточных.
    # 0.5 = случайная угадка, >0.6 = полезная модель, >0.7 = хорошо для trading.
    # Brier score: средняя квадратичная ошибка вероятности (p_edge vs реальный исход).
    # Чем ближе к baseline (Rate*(1-Rate)), тем меньше модель переоценивает/недооценивает.
    lines.append(f"  AUC: ранжирование (0.5=случайно, >0.6=полезно)")
    lines.append(f"  Brier: точность вер-стей (baseline={brier_baseline:.4f}, ниже=лучше)")
    lines.append("Fold   N   Pos   AUC   Brier")
    for fs in fold_stats:
        lines.append(f"  {fs['fold']}  {fs['n']:>4}   {fs['pos']:>3}  {fs['auc']:.3f}  {fs['brier']:.4f}")

    valid = oof_p > 0
    if valid.sum() > 10 and len(set(y[valid].tolist())) > 1:
        oof_auc = float(roc_auc_score(y[valid], oof_p[valid]))
        oof_brier = float(brier_score_loss(y[valid], oof_p[valid]))
        lines.append(f"OOF  {int(valid.sum()):>4}   {int(y[valid].sum()):>3}  {oof_auc:.3f}  {oof_brier:.4f}")

        if fold_stats:
            aucs = [fs["auc"] for fs in fold_stats]
            auc_range = max(aucs) - min(aucs)
            min_pos = min(fs["pos"] for fs in fold_stats)
            flag = "✓" if auc_range < 0.15 else "⚠"
            # Большой разброс AUC между фолдами → нестационарность рынка.
            # При range>0.15 модель работает стабильно только на части периодов.
            lines.append(f"{flag} AUC разброс={auc_range:.3f} (>0.15=нестационарность)  min-pos/fold={min_pos}")
    lines.append("")

    # 5.3 Precision@K
    lines.append("── 5.3 Точность в топ-K ──")
    # Prec@K: если взять K% сигналов с наибольшим p_edge — сколько из них реально прибыльны?
    # Lift = Prec@K / base_rate: во сколько раз лучше случайного выбора.
    # Для торговли: Lift≥2× = модель полезна, 1.2-2× = слабо, <1.2× = не лучше монеты.
    lines.append(f"  Prec@K = доля прибыльных в топ-K сигналах по p_edge")
    lines.append(f"  Lift = Prec/base ({pos_rate:.2%}) — во сколько раз лучше случайного")
    lines.append(f"  ✓≥2.0× хорошо  ⚠≥1.2× слабо  ✗<1.2× не лучше базы")
    lines.append("   k    n   prec   lift")
    base = float(y.mean()) if n > 0 else 1.0
    for k_frac in [0.01, 0.03, 0.05, 0.10]:
        k_n = max(1, int(n * k_frac))
        idx = np.argsort(oof_p)[::-1][:k_n]
        prec = float(np.mean(y[idx]))
        lift = prec / base if base > 0 else 0.0
        flag = "✓" if lift >= 2.0 else ("⚠" if lift >= 1.2 else "✗")
        lines.append(f" {k_frac*100:>3.0f}%  {k_n:>4}  {prec:.3f}  {lift:.1f}× {flag}")
    lines.append("")

    # 5.4 Calibration
    lines.append("── 5.4 Калибровка ──")
    # Таблица показывает: при p_edge в диапазоне [lo, hi] — какой реальный win-rate?
    # Δ = actual − p̂: если Δ>0 — модель недооценивает (осторожная), Δ<0 — переоценивает.
    # Для ранжирования сигналов калибровка не критична (нужен только порядок).
    # Для буквального использования p_edge как вероятности нужно ECE<0.05.
    lines.append("  Δ=actual−p̂: >0 недооценка (осторожная), <0 переоценка")
    lines.append("  ECE<0.05=хорошо; при ECE>0.10 p_edge — только для ранжирования")
    lines.append("Bucket    n    p̂    actual    Δ")
    max_gap = 0.0
    for i in range(10):
        lo, hi = i / 10, (i + 1) / 10
        mask = (oof_p >= lo) & (oof_p < hi) if i < 9 else (oof_p >= lo) & (oof_p <= hi)
        if mask.sum() == 0:
            continue
        pred_avg = float(np.mean(oof_p[mask]))
        actual_r = float(np.mean(y[mask]))
        delta = actual_r - pred_avg
        max_gap = max(max_gap, abs(delta))
        flag = "ok" if abs(delta) < 0.10 else ("⚠" if abs(delta) < 0.20 else "✗")
        lines.append(f"{lo:.1f}-{hi:.1f}  {mask.sum():>4}  {pred_avg:.3f}  {actual_r:.3f}  {delta:+.3f} {flag}")

    ece_val = 0.0
    if valid.sum() > 0:
        yv = y[valid].astype(float)
        pv = oof_p[valid]
        for i in range(10):
            lo, hi = i / 10, (i + 1) / 10
            mask = (pv >= lo) & (pv < hi) if i < 9 else (pv >= lo) & (pv <= hi)
            if mask.sum() == 0:
                continue
            ece_val += (mask.sum() / len(yv)) * abs(float(np.mean(yv[mask])) - float(np.mean(pv[mask])))

    cal_flag = "✓" if max_gap < 0.10 else ("⚠" if max_gap < 0.20 else "✗")
    lines.append(f"ECE={ece_val:.4f}  max_gap={max_gap:.3f} {cal_flag}")
    lines.append("")

    _append_train_metrics(lines, metrics)
    _append_status_line(lines, status, metrics)
    return "\n".join(lines)


def _append_train_metrics(lines: list[str], metrics: dict[str, Any]) -> None:
    lr_m = (metrics.get("lr") or {}).get("metrics") or {}
    gbdt_m = (metrics.get("gbdt") or {}).get("metrics") or {}
    if lr_m or gbdt_m:
        lines.append("── Метрики моделей ──")
        # LR (логистическая регрессия) — champion: быстрая, стабильная, хорошо калибруется.
        # GBDT (градиентный бустинг) — challenger: ловит нелинейности, но переобучается на малом n.
        # Если GBDT сильно обгоняет LR по AUC, это может быть переобучением — смотри фолды.
        lines.append("  LR=champion (быстро, стабильно)  GBDT=challenger (нелинейности)")
    if lr_m:
        ho = (metrics.get("lr") or {}).get("holdout_metrics") or {}
        ho_str = f"  holdout_AUC={ho['roc_auc']:.3f}" if ho.get("roc_auc") else ""
        lines.append(f"LR:   AUC={lr_m.get('roc_auc_mean', 0):.3f}  Brier={lr_m.get('brier_mean', 0):.4f}{ho_str}")
    if gbdt_m:
        lines.append(f"GBDT: AUC={gbdt_m.get('roc_auc_oof', 0):.3f}  Brier={gbdt_m.get('brier_oof', 0):.4f}  (OOF на всём датасете)")


def _append_status_line(lines: list[str], status: str, metrics: dict[str, Any]) -> None:
    promoted = (metrics.get("publish") or {}).get("promoted") or []
    skips = [
        (metrics.get("publish") or {}).get("promote_skip_lr"),
        (metrics.get("publish") or {}).get("promote_skip_gbdt"),
    ]
    lines.append("── Итог ──")
    if status == "ok":
        if promoted:
            kinds = ", ".join(p.get("kind", "?") for p in promoted)
            lines.append(f"✅ Промоутировано: [{kinds}]")
            # promoted = модель записана в cfg:ml_confirm:champion/challenger в режиме SHADOW.
            # Для перевода в enforce нужно вручную изменить mode=ENFORCE в Redis.
            lines.append("  Режим SHADOW — enforce включить вручную через Redis")
        else:
            skip_reason = next((s for s in skips if s), "candidate_only")
            lines.append(f"✅ Кандидат записан  (промоут пропущен: {skip_reason})")
            # candidate_only = V14_PROMOTE_AUTO=0. Модель сохранена в файл,
            # но в Redis cfg:ml_confirm не записана. Промоут выполняется вручную.
            lines.append("  cfg:ml_confirm не обновлён — для активации нужен ручной промоут")
    elif status == "skipped_small_dataset":
        lines.append("⚠ Пропущено — датасет слишком мал")
        min_rows = int(os.environ.get("V14_MIN_DATASET_ROWS", "500"))
        lines.append(f"  Нужно ≥{min_rows} строк (V14_MIN_DATASET_ROWS). Накопи больше меток.")
    else:
        lines.append(f"✗ Ошибка: {status}")


def _notify_train_report(
    *,
    redis_url: str,
    metrics: dict[str, Any],
    dataset_path: Path,
    tb_path: Path,
    status: str,
    elapsed_sec: float,
) -> None:
    """Best-effort: build validation report and push to notify:telegram stream."""
    try:
        text = _build_train_report_text(
            metrics=metrics,
            dataset_path=dataset_path,
            tb_path=tb_path,
            status=status,
            elapsed_sec=elapsed_sec,
        )
        if not text:
            logger.warning("notify_train_report: empty report, skipping")
            return
        import redis as _redis_mod
        r: Any = _redis_mod.Redis.from_url(redis_url, decode_responses=True)
        stream = _env("NOTIFY_TELEGRAM_STREAM", "notify:telegram")
        r.xadd(
            stream,
            {"type": "report", "subtype": "v14_of_train", "text": text, "ts": str(int(time.time() * 1000))},
            maxlen=200000,
            approximate=True,
        )
        logger.info("train report sent → %s", stream)
    except Exception as e:
        logger.warning("notify_train_report failed (best-effort): %s", e)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--work-dir", default=_env("V14_WORK_DIR", "/tmp/v14_of_train"))
    ap.add_argument("--out-dir", default=_env("V14_OUT_DIR", "/var/lib/trade/of_reports/models"))
    ap.add_argument("--auto-promote", type=int, default=_env_int("V14_PROMOTE_AUTO", 0))
    args = ap.parse_args([])

    work_dir = Path(args.work_dir)
    out_dir = Path(args.out_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    redis_url = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
    t0 = time.time()
    status = "ok"
    metrics: dict[str, Any] = {"started_at_ms": int(t0 * 1000)}

    paths = stage_export(
        redis_url=redis_url,
        inputs_stream=_env("V14_INPUTS_STREAM", "signals:of:inputs"),
        labels_stream=_env("V14_LABELS_STREAM", "labels:tb"),
        inputs_max=_env_int("V14_INPUTS_MAX_RECORDS", 5000),
        labels_max=_env_int("V14_LABELS_MAX_RECORDS", 5000),
        labels_since_hours=_env_int("V14_LABELS_SINCE_HOURS", 168),
        work_dir=work_dir,
    )
    if paths is None:
        logger.error("export stage failed")
        sys.exit(2)
    metrics["inputs_path"] = str(paths["inputs"])
    metrics["labels_path"] = str(paths["labels"])

    # Stage 3: cost-aware relabel
    tb_path = work_dir / "tb_labels.ndjson"
    relabel_info = stage_relabel(
        inputs_path=paths["inputs"],
        labels_path=paths["labels"],
        out_path=tb_path,
        fees_bps_one_side=_env_float("V14_FEES_BPS_ONE_SIDE", 2.0),
    )
    if relabel_info is None or relabel_info["processed"] == 0:
        logger.error("relabel stage failed or empty: %s", relabel_info)
        sys.exit(3)
    metrics["relabel"] = relabel_info

    # Stage 4: dataset build
    dataset_path = work_dir / "ml_dataset_v14.jsonl"
    ds_summary = stage_build_dataset(
        inputs_path=paths["inputs"],
        tb_labels_path=tb_path,
        out_path=dataset_path,
        y_label_col=_env("V14_Y_LABEL_COL", "y_edge_cost_aware"),
    )
    if ds_summary is None:
        logger.error("dataset build failed")
        sys.exit(4)
    metrics["dataset"] = ds_summary

    min_rows = _env_int("V14_MIN_DATASET_ROWS", 500)
    if int(ds_summary.get("joined_rows", 0)) < min_rows:
        logger.warning("dataset too small (%d < %d); skipping training", ds_summary.get("joined_rows"), min_rows)
        status = "skipped_small_dataset"
        metrics["status"] = status
        # still write summary to Redis for observability
    else:
        ts_str = time.strftime("%Y%m%d_%H%M%S", time.gmtime())

        # Stage 5: train LR baseline
        lr_info = stage_train_lr(dataset_path=dataset_path, out_dir=out_dir, ts_str=ts_str,
                                 holdout_hours=_env_int("V14_HOLDOUT_HOURS", 24))
        metrics["lr"] = lr_info

        # Stage 6: train GBDT challenger
        gbdt_info = stage_train_gbdt(dataset_path=dataset_path, out_dir=out_dir, ts_str=ts_str)
        metrics["gbdt"] = gbdt_info

        if lr_info is None and gbdt_info is None:
            logger.error("both LR and GBDT training failed")
            status = "fail_train"
        else:
            # Stage 7: publish
            pub_info = stage_publish(
                redis_url=redis_url,
                lr_info=lr_info,
                gbdt_info=gbdt_info,
                champion_key=_env("V14_CHAMPION_KEY", "cfg:ml_confirm:champion"),
                challenger_key=_env("V14_CHALLENGER_KEY", "cfg:ml_confirm:challenger"),
                auto_promote=bool(args.auto_promote),
                promote_brier_max=_env_float("V14_PROMOTE_BRIER_MAX", 0.20),
                promote_ece_max=_env_float("V14_PROMOTE_ECE_MAX", 0.10),
                promote_holdout_min_auc=_env_float("V14_PROMOTE_HOLDOUT_MIN_AUC", 0.55),
            )
            metrics["publish"] = pub_info

    # Final summary → Redis metrics key for observability / Prometheus exporter
    metrics["status"] = status
    metrics["elapsed_sec"] = round(time.time() - t0, 2)
    metrics["finished_at_ms"] = int(time.time() * 1000)

    metrics_key = _env("V14_TRAIN_METRICS_KEY", "metrics:v14_of_train:last")
    try:
        import redis
        r: Any = redis.Redis.from_url(redis_url, decode_responses=True)
        r.set(metrics_key, json.dumps(metrics, separators=(",", ":")))
        
        # Keep legacy SRE monitor happy (ml_sre_monitor.py expects these keys)
        r.set("meta_model:last_train_ts_ms", metrics["finished_at_ms"])
        if status == "ok":
            r.set("meta_model:last_status", "ok")
        else:
            r.set("meta_model:last_status", f"err:{status}")
            
        logger.info("wrote summary → %s (elapsed=%.1fs status=%s)", metrics_key, metrics["elapsed_sec"], status)
    except Exception as e:
        logger.warning("failed to write summary metrics to redis: %s", e)

    # Stage 8: send Telegram report (best-effort, does not affect exit code)
    if _env_int("V14_NOTIFY_ENABLED", 1):
        _notify_train_report(
            redis_url=redis_url,
            metrics=metrics,
            dataset_path=dataset_path,
            tb_path=tb_path,
            status=status,
            elapsed_sec=metrics["elapsed_sec"],
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
