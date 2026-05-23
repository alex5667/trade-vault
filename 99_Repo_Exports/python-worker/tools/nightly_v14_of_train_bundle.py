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
# v15_of (515 keys) = v14_of (359 keys) + 156 keys emitted by
# external_features_payload_v1.py that were silently dropped from training
# when the default was v14_of. Env `V14_FEATURE_SCHEMA_VER=v14_of` restores
# the old 359-key schema for comparison/rollback.
# Count is sourced live from core.ml_feature_schema_v{14,15}_of so totals
# never drift in this file.
# ---------------------------------------------------------------------------

# Schema version actually trained. Default 'v15_of' (515 keys, full ext_payload
# coverage). Override `V14_FEATURE_SCHEMA_VER=v14_of` to fall back to 359-key
# schema. Any other value falls back to v15_of with a warning.
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


def _load_dataset(path: Path) -> tuple[Any, Any]:
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
    return X, y


# ---------------------------------------------------------------------------
# Stage 5: train LR baseline → MetaModelLR JSON
# ---------------------------------------------------------------------------

def stage_train_lr(*, dataset_path: Path, out_dir: Path, ts_str: str) -> dict[str, Any] | None:
    """Train sklearn LR on v14_of dataset, write MetaModelLR-compatible JSON."""
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

    X, y = _load_dataset(dataset_path)
    if len(y) < 100 or int(y.sum()) < 10:
        logger.error("train_lr: insufficient data n=%d pos=%d", len(y), int(y.sum()))
        return None

    # 5-fold CV metrics
    auc_l: list[float] = []
    ap_l: list[float] = []
    brier_l: list[float] = []
    ll_l: list[float] = []
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    for tr, te in skf.split(X, y):
        sc_cv = StandardScaler().fit(X[tr])
        Xtr, Xte = sc_cv.transform(X[tr]), sc_cv.transform(X[te])
        lr_cv = LogisticRegression(C=1.0, max_iter=2000, class_weight="balanced", random_state=42)
        lr_cv.fit(Xtr, y[tr])
        p = lr_cv.predict_proba(Xte)[:, 1]
        if len(set(y[te])) > 1:
            auc_l.append(float(roc_auc_score(y[te], p)))
            ap_l.append(float(average_precision_score(y[te], p)))
        brier_l.append(float(brier_score_loss(y[te], p)))
        ll_l.append(float(log_loss(y[te], p, labels=[0, 1])))

    cv_metrics = {
        "roc_auc_mean": statistics.mean(auc_l) if auc_l else float("nan"),
        "pr_auc_mean": statistics.mean(ap_l) if ap_l else float("nan"),
        "brier_mean": statistics.mean(brier_l),
        "log_loss_mean": statistics.mean(ll_l),
        "n_rows": int(len(y)),
        "pos_rate": float(y.mean()),
    }

    # Final model on ALL data
    scaler = StandardScaler().fit(X)
    lr = LogisticRegression(C=1.0, max_iter=2000, class_weight="balanced", random_state=42)
    lr.fit(scaler.transform(X), y)

    robust_scaler_params = {
        feat: {
            "center": float(scaler.mean_[i]),
            "scale": float(scaler.scale_[i] if scaler.scale_[i] > 1e-9 else 1.0),
        }
        for i, feat in enumerate(V14_BASE_FEATURES)
    }

    pack = {
        "features": list(V14_BASE_FEATURES),
        "intercept": float(lr.intercept_[0]),
        "coef": [float(c) for c in lr.coef_[0]],
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

    X, y = _load_dataset(dataset_path)
    if len(y) < 200 or int(y.sum()) < 15:
        logger.error("train_gbdt: insufficient data n=%d pos=%d", len(y), int(y.sum()))
        return None

    # OOF predictions to train meta on unbiased base outputs.
    oof_p_lr = np.zeros(len(y), dtype=np.float64)
    oof_p_gbdt = np.zeros(len(y), dtype=np.float64)
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    for tr, te in skf.split(X, y):
        sc = StandardScaler().fit(X[tr])
        Xtr, Xte = sc.transform(X[tr]), sc.transform(X[te])
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
    Z = np.column_stack([oof_p_lr, oof_p_gbdt])
    meta_lr_model = LogisticRegression(C=1.0, max_iter=2000, class_weight="balanced", random_state=42)
    meta_lr_model.fit(Z, y)

    # OOF stack predictions for evaluation
    p_meta_oof = meta_lr_model.predict_proba(Z)[:, 1]
    metrics = {
        "roc_auc_oof": float(roc_auc_score(y, p_meta_oof)),
        "pr_auc_oof": float(average_precision_score(y, p_meta_oof)),
        "brier_oof": float(brier_score_loss(y, p_meta_oof)),
        "log_loss_oof": float(log_loss(y, p_meta_oof, labels=[0, 1])),
        "n_rows": int(len(y)),
        "pos_rate": float(y.mean()),
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
                  promote_brier_max: float, promote_ece_max: float) -> dict[str, Any]:
    """Always writes candidate keys; updates champion/challenger only if gates pass."""
    try:
        import redis
        r = redis.Redis.from_url(redis_url, decode_responses=True)
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

    # Promotion gates
    def _pass_lr() -> tuple[bool, str]:
        if not lr_info:
            return False, "no_lr_info"
        m = lr_info.get("metrics", {})
        br = float(m.get("brier_mean", 1.0))
        if br > promote_brier_max:
            return False, f"brier_too_high({br:.4f}>{promote_brier_max:.4f})"
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
            # backup previous + promote
            try:
                prev = r.get(champion_key)
                if prev:
                    r.set(champion_key + "_prev_nightly", prev)
            except Exception:
                pass
            cfg = {
                "schema_version": 1,
                "kind": "meta_lr",
                "run_id": lr_info["run_id"],
                "created_ms": int(time.time() * 1000),
                "model_path": lr_info["path"],
                "mode": "SHADOW",   # auto-promote keeps SHADOW; human flips to ENFORCE
                "enforce_share": 0.0,
                "p_min": 0.5,
                "feature_schema_ver": _FEATURE_SCHEMA_VER,
                "fail_policy": "OPEN",
                "model_signature": lr_info.get("signature", ""),
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
                    r.set(challenger_key + "_prev_nightly", prev)
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
            }
            r.set(challenger_key, json.dumps(cfg, separators=(",", ":")))
            result["promoted"].append({"key": challenger_key, "kind": "edge_stack_v1"})
        else:
            result["promote_skip_gbdt"] = reason

    return result


# ---------------------------------------------------------------------------
# Stage 1+2: export streams to NDJSON
# ---------------------------------------------------------------------------

def stage_export(*, redis_url: str, inputs_stream: str, labels_stream: str,
                 inputs_max: int, labels_max: int, work_dir: Path) -> dict[str, Path] | None:
    inputs_path = work_dir / "of_inputs.ndjson"
    labels_path = work_dir / "labels_tb_live.ndjson"

    ok1 = _run_cmd([
        sys.executable, "-m", "tools.export_of_inputs_ndjson",
        "--redis-url", redis_url,
        "--out", str(inputs_path),
        "--max-records", str(inputs_max),
    ], log_tag="export_inputs")

    ok2 = _run_cmd([
        sys.executable, "-m", "tools.export_stream_payload_ndjson_v1",
        "--redis-url", redis_url,
        "--stream", labels_stream,
        "--payload-field", "payload",
        "--out", str(labels_path),
        "--since-hours", "72",
        "--max-scan", str(labels_max),
    ], log_tag="export_labels")

    if not (ok1 and ok2 and inputs_path.exists() and labels_path.exists()):
        return None

    return {"inputs": inputs_path, "labels": labels_path}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--work-dir", default=_env("V14_WORK_DIR", "/tmp/v14_of_train"))
    ap.add_argument("--out-dir", default=_env("V14_OUT_DIR", "/var/lib/trade/of_reports/models"))
    ap.add_argument("--auto-promote", type=int, default=_env_int("V14_PROMOTE_AUTO", 0))
    args = ap.parse_args()

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
        lr_info = stage_train_lr(dataset_path=dataset_path, out_dir=out_dir, ts_str=ts_str)
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
            )
            metrics["publish"] = pub_info

    # Final summary → Redis metrics key for observability / Prometheus exporter
    metrics["status"] = status
    metrics["elapsed_sec"] = round(time.time() - t0, 2)
    metrics["finished_at_ms"] = int(time.time() * 1000)

    metrics_key = _env("V14_TRAIN_METRICS_KEY", "metrics:v14_of_train:last")
    try:
        import redis
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        r.set(metrics_key, json.dumps(metrics, separators=(",", ":")))
        logger.info("wrote summary → %s (elapsed=%.1fs status=%s)", metrics_key, metrics["elapsed_sec"], status)
    except Exception as e:
        logger.warning("failed to write summary metrics to redis: %s", e)

    sys.exit(0)
