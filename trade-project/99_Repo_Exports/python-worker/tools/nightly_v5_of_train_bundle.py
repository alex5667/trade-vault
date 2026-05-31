"""Full-feature v5_of training bundle — 210-key external_features set + strong regularization.

Pipeline:
  1. Reuse existing v14_of-exported dataset (ml_dataset_v14.jsonl)
     OR re-export if --reexport flag set.
  2. Load all 210 v5_of features from external_features_payload_v1.external_feature_keys().
  3. Train LR (strong L2, balanced) + GBDT (shallow trees, subsample) — 5-fold StratifiedKFold.
  4. Write artifacts to /var/lib/trade/ml_models/v5_of_<ts>/ — DOES NOT touch champion config.
  5. Honest reporting: n_features, n_pos, overfit indicators (train AUC vs CV AUC).

Designed for n_features >> n_samples regime. Default knobs are conservative.

Env vars (all optional):
  REDIS_URL                       redis://redis-worker-1:6379/0
  V5OF_DATASET_PATH               /var/lib/trade/of_reports/v14_of_train_work/ml_dataset_v14.jsonl
  V5OF_OUT_ROOT                   /var/lib/trade/ml_models
  V5OF_LR_C                       0.05    (strong L2; lower = stronger regularization)
  V5OF_GBDT_MAX_DEPTH             2
  V5OF_GBDT_N_EST                 50
  V5OF_GBDT_LR                    0.05
  V5OF_GBDT_SUBSAMPLE             0.7
  V5OF_MIN_POS                    20
  V5OF_LABEL_COL                  y_edge_cost_aware
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import statistics
import sys
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("nightly_v5_of_train")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_float(k: str, d: float) -> float:
    try:
        return float(_env(k, str(d)))
    except Exception:
        return d


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


def _coerce(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        if isinstance(v, bool):
            return 1.0 if v else 0.0
        return float(v)
    except Exception:
        return 0.0


def load_dataset(path: Path, feature_names: list[str], label_col: str) -> tuple[Any, Any, dict[str, float]]:
    """Return (X, y, coverage_per_feature)."""
    import numpy as np

    rows: list[dict[str, Any]] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue

    logger.info("loaded %d rows from %s", len(rows), path)

    n = len(rows)
    n_feat = len(feature_names)
    X = np.zeros((n, n_feat), dtype=np.float64)
    y = np.zeros(n, dtype=np.int64)
    cov: dict[str, int] = {k: 0 for k in feature_names}

    for i, r in enumerate(rows):
        ind = r.get("indicators") or {}
        if isinstance(ind, str):
            try:
                ind = json.loads(ind)
            except Exception:
                ind = {}
        y[i] = int(r.get(label_col, 0) or 0)
        for j, k in enumerate(feature_names):
            v = ind.get(k)
            if v is not None:
                cov[k] += 1
            X[i, j] = _coerce(v)

    cov_pct = {k: round(c / max(n, 1), 4) for k, c in cov.items()}
    return X, y, cov_pct


def train_lr(X: Any, y: Any, feature_names: list[str], C: float) -> dict[str, Any]:
    import numpy as np
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import (average_precision_score, brier_score_loss,
                                 log_loss, roc_auc_score)
    from sklearn.model_selection import StratifiedKFold
    from sklearn.preprocessing import StandardScaler

    n_splits = min(5, int(y.sum()))
    if n_splits < 2:
        raise RuntimeError(f"too few positives ({int(y.sum())}) for CV")

    auc_l: list[float] = []
    ap_l: list[float] = []
    brier_l: list[float] = []
    ll_l: list[float] = []
    train_auc_l: list[float] = []

    skf = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
    for tr, te in skf.split(X, y):
        sc = StandardScaler().fit(X[tr])
        Xtr, Xte = sc.transform(X[tr]), sc.transform(X[te])
        lr = LogisticRegression(
            C=C, max_iter=3000, class_weight="balanced",
            random_state=42, solver="liblinear", penalty="l2",
        )
        lr.fit(Xtr, y[tr])
        p_te = lr.predict_proba(Xte)[:, 1]
        p_tr = lr.predict_proba(Xtr)[:, 1]
        if len(set(y[te])) > 1:
            auc_l.append(float(roc_auc_score(y[te], p_te)))
            ap_l.append(float(average_precision_score(y[te], p_te)))
        if len(set(y[tr])) > 1:
            train_auc_l.append(float(roc_auc_score(y[tr], p_tr)))
        brier_l.append(float(brier_score_loss(y[te], p_te)))
        ll_l.append(float(log_loss(y[te], p_te, labels=[0, 1])))

    # Final on all
    sc = StandardScaler().fit(X)
    lr_final = LogisticRegression(
        C=C, max_iter=3000, class_weight="balanced",
        random_state=42, solver="liblinear", penalty="l2",
    )
    lr_final.fit(sc.transform(X), y)

    coef = lr_final.coef_[0]
    top_features = sorted(
        zip(feature_names, [float(c) for c in coef]),
        key=lambda kv: -abs(kv[1]),
    )[:20]

    return {
        "C": C,
        "n_features": int(X.shape[1]),
        "n_splits": n_splits,
        "cv": {
            "roc_auc_mean": statistics.mean(auc_l) if auc_l else float("nan"),
            "pr_auc_mean": statistics.mean(ap_l) if ap_l else float("nan"),
            "brier_mean": statistics.mean(brier_l),
            "log_loss_mean": statistics.mean(ll_l),
            "train_auc_mean": statistics.mean(train_auc_l) if train_auc_l else float("nan"),
            "overfit_gap_auc": (statistics.mean(train_auc_l) - statistics.mean(auc_l)) if (train_auc_l and auc_l) else float("nan"),
        },
        "top20_coef_abs": top_features,
        "intercept": float(lr_final.intercept_[0]),
        "coef_nnz": int((np.abs(coef) > 1e-6).sum()),
    }


def train_gbdt(X: Any, y: Any, feature_names: list[str],
               max_depth: int, n_estimators: int, lr_rate: float, subsample: float) -> dict[str, Any]:
    import joblib
    import numpy as np
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.metrics import (average_precision_score, brier_score_loss,
                                 log_loss, roc_auc_score)
    from sklearn.model_selection import StratifiedKFold

    n_splits = min(5, int(y.sum()))
    if n_splits < 2:
        raise RuntimeError(f"too few positives for GBDT CV")

    oof_pred = np.zeros(len(y), dtype=np.float64)
    skf = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
    for tr, te in skf.split(X, y):
        gb = GradientBoostingClassifier(
            max_depth=max_depth, n_estimators=n_estimators,
            learning_rate=lr_rate, subsample=subsample,
            random_state=42, max_features="sqrt",
        )
        gb.fit(X[tr], y[tr])
        oof_pred[te] = gb.predict_proba(X[te])[:, 1]

    metrics = {
        "roc_auc_oof": float(roc_auc_score(y, oof_pred)),
        "pr_auc_oof": float(average_precision_score(y, oof_pred)),
        "brier_oof": float(brier_score_loss(y, oof_pred)),
        "log_loss_oof": float(log_loss(y, oof_pred, labels=[0, 1])),
        "n_rows": len(y),
        "pos_rate": float(y.mean()),
        "n_features": int(X.shape[1]),
    }

    # Final model on all data
    gb_final = GradientBoostingClassifier(
        max_depth=max_depth, n_estimators=n_estimators,
        learning_rate=lr_rate, subsample=subsample,
        random_state=42, max_features="sqrt",
    )
    gb_final.fit(X, y)
    importances = sorted(
        zip(feature_names, [float(v) for v in gb_final.feature_importances_]),
        key=lambda kv: -kv[1],
    )[:20]
    return {
        "params": {
            "max_depth": max_depth, "n_estimators": n_estimators,
            "learning_rate": lr_rate, "subsample": subsample,
        },
        "metrics": metrics,
        "top20_importance": importances,
        "_model": gb_final,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default=_env("V5OF_DATASET_PATH",
                    "/var/lib/trade/of_reports/v14_of_train_work/ml_dataset_v14.jsonl"))
    ap.add_argument("--out-root", default=_env("V5OF_OUT_ROOT", "/var/lib/trade/ml_models"))
    ap.add_argument("--lr-c", type=float, default=_env_float("V5OF_LR_C", 0.05))
    ap.add_argument("--gbdt-max-depth", type=int, default=_env_int("V5OF_GBDT_MAX_DEPTH", 2))
    ap.add_argument("--gbdt-n-est", type=int, default=_env_int("V5OF_GBDT_N_EST", 50))
    ap.add_argument("--gbdt-lr", type=float, default=_env_float("V5OF_GBDT_LR", 0.05))
    ap.add_argument("--gbdt-subsample", type=float, default=_env_float("V5OF_GBDT_SUBSAMPLE", 0.7))
    ap.add_argument("--min-pos", type=int, default=_env_int("V5OF_MIN_POS", 20))
    ap.add_argument("--label-col", default=_env("V5OF_LABEL_COL", "y_edge_cost_aware"))
    args = ap.parse_args()

    from core.external_features_payload_v1 import external_feature_keys
    feat = list(external_feature_keys())
    logger.info("v5_of feature set: %d keys (external_features_payload_v1)", len(feat))

    ds_path = Path(args.dataset)
    if not ds_path.exists():
        logger.error("dataset not found: %s", ds_path)
        return 2

    X, y, cov = load_dataset(ds_path, feat, args.label_col)
    n_pos = int(y.sum())
    n_total = len(y)
    pos_rate = n_pos / max(n_total, 1)
    logger.info("dataset: n=%d pos=%d pos_rate=%.4f n_features=%d", n_total, n_pos, pos_rate, len(feat))

    if n_pos < args.min_pos:
        logger.error("insufficient positives: %d < %d (V5OF_MIN_POS)", n_pos, args.min_pos)
        return 3

    # Coverage report: top under-covered features (< 10%)
    low_cov = sorted([(k, v) for k, v in cov.items() if v < 0.10], key=lambda kv: kv[1])[:20]
    high_cov = sum(1 for v in cov.values() if v >= 0.90)
    logger.info("coverage: %d/%d features ≥90%% present; %d <10%% (model leans on these less)",
                high_cov, len(feat), sum(1 for v in cov.values() if v < 0.10))

    ts_str = time.strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_root) / f"v5_of_{ts_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info("output dir: %s", out_dir)

    logger.info("training LR (C=%s, balanced, L2) ...", args.lr_c)
    lr_report = train_lr(X, y, feat, args.lr_c)
    logger.info("LR cv: roc_auc=%.4f pr_auc=%.4f overfit_gap=%.4f nnz=%d",
                lr_report["cv"]["roc_auc_mean"], lr_report["cv"]["pr_auc_mean"],
                lr_report["cv"]["overfit_gap_auc"], lr_report["coef_nnz"])

    logger.info("training GBDT (max_depth=%d, n_est=%d, lr=%.3f, subsample=%.2f) ...",
                args.gbdt_max_depth, args.gbdt_n_est, args.gbdt_lr, args.gbdt_subsample)
    gb_report = train_gbdt(X, y, feat, args.gbdt_max_depth, args.gbdt_n_est,
                           args.gbdt_lr, args.gbdt_subsample)
    logger.info("GBDT oof: roc_auc=%.4f pr_auc=%.4f", gb_report["metrics"]["roc_auc_oof"],
                gb_report["metrics"]["pr_auc_oof"])

    # Persist GBDT joblib
    import joblib
    gb_model = gb_report.pop("_model")
    gb_path = out_dir / "edge_stack_v5_of.joblib"
    joblib.dump({
        "model": gb_model,
        "feature_names": feat,
        "feature_schema_ver": "v5_of",
        "schema_hash": "v5_of_external_keys_2026_05_16",
        "feature_cols_hash": hashlib.sha256(",".join(feat).encode("utf-8")).hexdigest()[:16],
        "metrics": gb_report["metrics"],
        "params": gb_report["params"],
        "created_ms": int(time.time() * 1000),
    }, gb_path)
    logger.info("wrote GBDT: %s", gb_path)

    summary = {
        "ts": ts_str,
        "dataset": str(ds_path),
        "n_rows": n_total,
        "n_pos": n_pos,
        "pos_rate": pos_rate,
        "n_features": len(feat),
        "feature_cols_hash": hashlib.sha256(",".join(feat).encode("utf-8")).hexdigest()[:16],
        "lr": lr_report,
        "gbdt": gb_report,
        "coverage": {
            "n_features_ge_90pct": high_cov,
            "low_coverage_top20": low_cov,
        },
        "warning": (
            "n_features={} >> n_pos={}: model heavily underdetermined; "
            "metrics are CV/OOF but interpret with caution (need 7-14d more data)"
        ).format(len(feat), n_pos),
    }

    summary_path = out_dir / "v5_of_report.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    logger.info("wrote summary: %s", summary_path)

    short_summary = {
        "ts_ms": int(time.time() * 1000),
        "ts_str": ts_str,
        "n_rows": n_total,
        "n_pos": n_pos,
        "pos_rate": pos_rate,
        "n_features": len(feat),
        "lr_cv_roc_auc": lr_report["cv"]["roc_auc_mean"],
        "lr_cv_pr_auc": lr_report["cv"]["pr_auc_mean"],
        "lr_overfit_gap": lr_report["cv"]["overfit_gap_auc"],
        "lr_coef_nnz": lr_report["coef_nnz"],
        "gbdt_oof_roc_auc": gb_report["metrics"]["roc_auc_oof"],
        "gbdt_oof_pr_auc": gb_report["metrics"]["pr_auc_oof"],
        "out_dir": str(out_dir),
        "status": "ok",
    }
    print(json.dumps(short_summary, indent=2))

    # Publish to Redis for SRE/monitoring (mirrors v14_of_train:last pattern).
    redis_url = os.environ.get("REDIS_URL", "")
    if redis_url:
        try:
            import redis as _redis
            r = _redis.from_url(redis_url, socket_timeout=3.0)
            r.set("metrics:v5_of_train:last", json.dumps(short_summary, separators=(",", ":")))
            logger.info("wrote metrics:v5_of_train:last")
        except Exception as e:
            logger.warning("failed to publish metrics to Redis: %s", e)
    return 0


def loop_main() -> int:
    """Run main() in a loop every V5OF_TRAIN_INTERVAL_SEC seconds.

    Designed for the scanner-v5-of-train-timer container. Mirrors the
    v14_of_train_timer pattern (6h interval by default).
    """
    import signal as _signal
    interval = int(os.environ.get("V5OF_TRAIN_INTERVAL_SEC", "21600") or "21600")
    enabled = os.environ.get("V5OF_TRAIN_ENABLED", "1") == "1"
    stop = {"flag": False}

    def _sig(_a, _b):
        stop["flag"] = True

    _signal.signal(_signal.SIGTERM, _sig)
    _signal.signal(_signal.SIGINT, _sig)

    logger.info(
        "v5_of_train_timer starting (interval=%ds, enabled=%s)",
        interval, enabled,
    )
    while not stop["flag"]:
        if not enabled:
            logger.info("V5OF_TRAIN_ENABLED=0, skipping cycle")
        else:
            t0 = time.time()
            try:
                rc = main()
                dt = time.time() - t0
                logger.info("cycle done in %.1fs (rc=%d)", dt, rc)
            except SystemExit as e:
                logger.warning("cycle exited with code %s", e.code)
            except Exception as e:
                logger.exception("cycle failed: %s", e)
                # Publish error status so SRE sees the failure.
                redis_url = os.environ.get("REDIS_URL", "")
                if redis_url:
                    try:
                        import redis as _redis
                        r = _redis.from_url(redis_url, socket_timeout=3.0)
                        r.set("metrics:v5_of_train:last", json.dumps({
                            "ts_ms": int(time.time() * 1000),
                            "status": "error",
                            "error": str(e)[:500],
                        }, separators=(",", ":")))
                    except Exception:
                        pass
        # Sleep with cooperative stop.
        for _ in range(interval):
            if stop["flag"]:
                break
            time.sleep(1)
    logger.info("v5_of_train_timer stopped")
    return 0


if __name__ == "__main__":
    if "--loop" in sys.argv:
        sys.argv = [a for a in sys.argv if a != "--loop"]
        sys.exit(loop_main())
    sys.exit(main())
