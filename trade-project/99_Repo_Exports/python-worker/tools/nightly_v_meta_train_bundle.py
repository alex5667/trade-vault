"""Meta-stacking trainer: blend v14_of (LR, 22 features) and v5_of (GBDT, 210 features).

Pipeline:
  1. Load shared dataset (ml_dataset_v14.jsonl) built by v14-of-train-timer.
  2. Build two feature matrices: X14 (V14_BASE_FEATURES) and X5 (v5_of full set).
  3. K-fold StratifiedKFold(5) with FIXED random_state=42:
       For each fold:
         - Train v14 LR on fold-train, predict OOF on fold-test → oof_v14
         - Train v5 GBDT on fold-train, predict OOF on fold-test → oof_v5
  4. Train meta-LR on (oof_v14, oof_v5) → final p_blend.
  5. Report AUC(v14), AUC(v5), AUC(meta), uplift vs both children.
  6. Persist meta artifact to disk + Redis (`cfg:ml_confirm:meta_lr_blend:candidate`).
     NEVER touches champion — manual `make ml-approve` required to promote.

ENV (all optional):
  REDIS_URL                  redis://redis-worker-1:6379/0
  V_META_DATASET_PATH        /var/lib/trade/of_reports/v14_of_train_work/ml_dataset_v14.jsonl
  V_META_OUT_ROOT            /var/lib/trade/ml_models
  V_META_LABEL_COL           y_edge_cost_aware
  V_META_TRAIN_INTERVAL_SEC  21600  (6h)
  V_META_TRAIN_ENABLED       1
  V_META_CANDIDATE_KEY       cfg:ml_confirm:meta_lr_blend:candidate
  V_META_METRICS_KEY         metrics:v_meta_train:last
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("nightly_v_meta_train")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# Mirror of V14_BASE_FEATURES from tools/nightly_v14_of_train_bundle.py:239
# Kept inline for self-containment.
V14_BASE_FEATURES: list[str] = [
    "delta_z", "ofi_z", "ofi_stability_score", "spread_bps", "expected_slippage_bps",
    "of_score_final", "of_score_final_raw",
    "strong_gate_have", "strong_gate_need",
    "weak_progress", "sweep_recent", "reclaim_recent", "obi_stable",
    "iceberg_strict", "abs_lvl_ok",
    "liq_score", "liq_spread_bps", "liq_book_rate_hz",
    "pressure_per_min_ema", "cooldown_hit_rate_ema",
    "confidence",
]


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _coerce(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        if isinstance(v, bool):
            return 1.0 if v else 0.0
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def load_dataset(path: Path, label_col: str) -> tuple[Any, Any, Any, list[str]]:
    """Return (X14, X5, y, v5_keys) — two feature matrices + labels + v5 key order."""
    import numpy as np
    from core.external_features_payload_v1 import _NUM_KEYS, _BOOL_KEYS

    v5_keys = list(_NUM_KEYS) + list(_BOOL_KEYS)

    rows: list[dict[str, Any]] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue

    n = len(rows)
    X14 = np.zeros((n, len(V14_BASE_FEATURES)), dtype=np.float64)
    X5 = np.zeros((n, len(v5_keys)), dtype=np.float64)
    y = np.zeros(n, dtype=np.int64)

    for i, r in enumerate(rows):
        ind = r.get("indicators") or {}
        if isinstance(ind, str):
            try:
                ind = json.loads(ind)
            except Exception:
                ind = {}
        y[i] = int(r.get(label_col, 0) or 0)
        for j, k in enumerate(V14_BASE_FEATURES):
            X14[i, j] = _coerce(ind.get(k))
        for j, k in enumerate(v5_keys):
            X5[i, j] = _coerce(ind.get(k))

    logger.info("loaded %d rows, X14=%s X5=%s pos=%d (%.4f)",
                n, X14.shape, X5.shape, int(y.sum()), float(y.mean()))
    return X14, X5, y, v5_keys


def kfold_oof(X14: Any, X5: Any, y: Any, n_splits: int = 5) -> tuple[Any, Any]:
    """Return (oof_v14, oof_v5) — out-of-fold probabilities."""
    import numpy as np
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import StratifiedKFold
    from sklearn.preprocessing import StandardScaler

    oof_v14 = np.zeros(len(y), dtype=np.float64)
    oof_v5 = np.zeros(len(y), dtype=np.float64)

    n_pos = int(y.sum())
    n_splits = min(n_splits, n_pos)
    if n_splits < 2:
        raise RuntimeError(f"need >=2 positives per fold; got {n_pos} total")

    skf = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
    for fold_idx, (tr, te) in enumerate(skf.split(X14, y)):
        # v14: LR with standard scaler (C=1.0, balanced) — mirrors v14_of_bundle.
        sc = StandardScaler().fit(X14[tr])
        lr = LogisticRegression(
            C=1.0, max_iter=2000, class_weight="balanced",
            random_state=42, solver="liblinear",
        )
        lr.fit(sc.transform(X14[tr]), y[tr])
        oof_v14[te] = lr.predict_proba(sc.transform(X14[te]))[:, 1]

        # v5: shallow GBDT (mirrors v5_of_bundle defaults).
        gb = GradientBoostingClassifier(
            max_depth=2, n_estimators=50, learning_rate=0.05,
            subsample=0.7, max_features="sqrt", random_state=42,
        )
        gb.fit(X5[tr], y[tr])
        oof_v5[te] = gb.predict_proba(X5[te])[:, 1]

        logger.debug("fold %d/%d trained", fold_idx + 1, n_splits)

    return oof_v14, oof_v5


def train_meta_lr(oof_v14: Any, oof_v5: Any, y: Any) -> dict[str, Any]:
    """Train meta-LR on (p_v14, p_v5) → returns artifact dict + metrics."""
    import numpy as np
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import (average_precision_score, brier_score_loss,
                                 log_loss, roc_auc_score)
    from sklearn.model_selection import StratifiedKFold

    X_meta = np.column_stack([oof_v14, oof_v5])
    # Note: meta uses raw OOF probabilities (no scaler — already on [0,1]).
    n_pos = int(y.sum())
    n_splits = min(5, n_pos)

    # CV the meta-LR itself to get honest meta AUC.
    p_meta_oof = np.zeros(len(y), dtype=np.float64)
    skf = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=43)  # different seed
    for tr, te in skf.split(X_meta, y):
        m = LogisticRegression(
            C=1.0, max_iter=2000, class_weight="balanced",
            random_state=42, solver="liblinear",
        )
        m.fit(X_meta[tr], y[tr])
        p_meta_oof[te] = m.predict_proba(X_meta[te])[:, 1]

    # Final meta-LR fit on all OOF data (for production inference).
    meta_final = LogisticRegression(
        C=1.0, max_iter=2000, class_weight="balanced",
        random_state=42, solver="liblinear",
    )
    meta_final.fit(X_meta, y)

    metrics = {
        "auc_v14": float(roc_auc_score(y, oof_v14)),
        "auc_v5": float(roc_auc_score(y, oof_v5)),
        "auc_meta": float(roc_auc_score(y, p_meta_oof)),
        "pr_auc_v14": float(average_precision_score(y, oof_v14)),
        "pr_auc_v5": float(average_precision_score(y, oof_v5)),
        "pr_auc_meta": float(average_precision_score(y, p_meta_oof)),
        "brier_meta": float(brier_score_loss(y, p_meta_oof)),
        "logloss_meta": float(log_loss(y, p_meta_oof, labels=[0, 1])),
        "n_rows": len(y),
        "n_pos": n_pos,
        "pos_rate": float(y.mean()),
    }
    metrics["uplift_meta_vs_v14_auc"] = metrics["auc_meta"] - metrics["auc_v14"]
    metrics["uplift_meta_vs_v5_auc"] = metrics["auc_meta"] - metrics["auc_v5"]

    artifact = {
        "kind": "meta_lr_blend",
        "schema_version": 2,
        "intercept": float(meta_final.intercept_[0]),
        "coef_v14": float(meta_final.coef_[0][0]),
        "coef_v5": float(meta_final.coef_[0][1]),
        "feature_names": ["p_v14", "p_v5"],
        "metrics": metrics,
    }
    return artifact


def train_final_children(X14: Any, X5: Any, y: Any) -> dict[str, Any]:
    """Re-fit child models on the full dataset for inference-time scoring.

    The K-fold pass produces only OOF predictions; the meta-LR is trained on those.
    For production inference we need single deterministic child models — those are
    fit here on all rows with the same hyperparameters used per-fold.
    """
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler

    scaler_v14 = StandardScaler().fit(X14)
    lr_v14 = LogisticRegression(
        C=1.0, max_iter=2000, class_weight="balanced",
        random_state=42, solver="liblinear",
    )
    lr_v14.fit(scaler_v14.transform(X14), y)

    gb_v5 = GradientBoostingClassifier(
        max_depth=2, n_estimators=50, learning_rate=0.05,
        subsample=0.7, max_features="sqrt", random_state=42,
    )
    gb_v5.fit(X5, y)

    return {"lr_v14": lr_v14, "scaler_v14": scaler_v14, "gb_v5": gb_v5}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default=_env(
        "V_META_DATASET_PATH",
        "/var/lib/trade/of_reports/v14_of_train_work/ml_dataset_v14.jsonl",
    ))
    ap.add_argument("--out-root", default=_env("V_META_OUT_ROOT", "/var/lib/trade/ml_models"))
    ap.add_argument("--label-col", default=_env("V_META_LABEL_COL", "y_edge_cost_aware"))
    args = ap.parse_args()

    ds_path = Path(args.dataset)
    if not ds_path.exists():
        logger.error("dataset not found: %s", ds_path)
        return 2

    X14, X5, y, v5_keys = load_dataset(ds_path, args.label_col)
    if int(y.sum()) < 5:
        logger.error("insufficient positives: %d < 5", int(y.sum()))
        return 3

    logger.info("computing OOF predictions (5-fold) for v14 and v5 children ...")
    oof_v14, oof_v5 = kfold_oof(X14, X5, y)

    logger.info("training meta-LR on (p_v14, p_v5) ...")
    art = train_meta_lr(oof_v14, oof_v5, y)
    m = art["metrics"]
    logger.info(
        "AUC: v14=%.4f v5=%.4f meta=%.4f | uplift_v14=%+.4f uplift_v5=%+.4f | coef_v14=%.3f coef_v5=%.3f",
        m["auc_v14"], m["auc_v5"], m["auc_meta"],
        m["uplift_meta_vs_v14_auc"], m["uplift_meta_vs_v5_auc"],
        art["coef_v14"], art["coef_v5"],
    )

    logger.info("re-fitting final child models on full dataset for inference ...")
    children = train_final_children(X14, X5, y)

    # Persist artifact + child models to disk.
    ts_str = time.strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_root) / f"meta_lr_blend_{ts_str}"
    out_dir.mkdir(parents=True, exist_ok=True)

    import joblib
    lr_v14_path = out_dir / "lr_v14_final.joblib"
    scaler_v14_path = out_dir / "scaler_v14_final.joblib"
    gb_v5_path = out_dir / "gb_v5_final.joblib"
    joblib.dump(children["lr_v14"], lr_v14_path)
    joblib.dump(children["scaler_v14"], scaler_v14_path)
    joblib.dump(children["gb_v5"], gb_v5_path)
    logger.info("wrote child models: %s, %s, %s", lr_v14_path, scaler_v14_path, gb_v5_path)

    art_full = dict(art)
    art_full["run_id"] = f"meta_lr_blend_{ts_str}"
    art_full["created_ms"] = int(time.time() * 1000)
    art_full["feature_cols_hash_v14"] = hashlib.sha256(
        ",".join(V14_BASE_FEATURES).encode("utf-8")
    ).hexdigest()[:16]
    art_full["feature_cols_hash_v5"] = hashlib.sha256(
        ",".join(v5_keys).encode("utf-8")
    ).hexdigest()[:16]
    art_full["dataset_path"] = str(ds_path)
    art_full["child_models"] = {
        "v14": {
            "model_path": str(lr_v14_path),
            "scaler_path": str(scaler_v14_path),
            "features": list(V14_BASE_FEATURES),
        },
        "v5": {
            "model_path": str(gb_v5_path),
            "scaler_path": None,
            "features": list(v5_keys),
        },
    }

    artifact_path = out_dir / "meta_lr_blend.json"
    artifact_path.write_text(json.dumps(art_full, indent=2, ensure_ascii=False))
    logger.info("wrote artifact: %s", artifact_path)

    # Publish to Redis as CANDIDATE (NEVER champion).
    redis_url = os.environ.get("REDIS_URL", "")
    candidate_key = os.environ.get("V_META_CANDIDATE_KEY", "cfg:ml_confirm:meta_lr_blend:candidate")
    metrics_key = os.environ.get("V_META_METRICS_KEY", "metrics:v_meta_train:last")
    if redis_url:
        try:
            import redis as _redis
            r = _redis.from_url(redis_url, socket_timeout=3.0)
            candidate = {
                "kind": "meta_lr_blend",
                "schema_version": 2,
                "run_id": art_full["run_id"],
                "mode": "SHADOW",
                "model_path": str(artifact_path),
                "intercept": art_full["intercept"],
                "coef_v14": art_full["coef_v14"],
                "coef_v5": art_full["coef_v5"],
                "metrics": m,
                "created_ms": art_full["created_ms"],
            }
            r.set(candidate_key, json.dumps(candidate, separators=(",", ":")))
            r.set(metrics_key, json.dumps({
                "ts_str": ts_str,
                "ts_ms": art_full["created_ms"],
                "status": "ok",
                **m,
            }, separators=(",", ":")))
            logger.info("wrote %s + %s", candidate_key, metrics_key)
        except Exception as e:
            logger.warning("Redis publish failed: %s", e)

    print(json.dumps({
        "run_id": art_full["run_id"],
        **m,
    }, indent=2))
    return 0


def loop_main() -> int:
    import signal as _signal
    interval = int(os.environ.get("V_META_TRAIN_INTERVAL_SEC", "21600") or "21600")
    enabled = os.environ.get("V_META_TRAIN_ENABLED", "1") == "1"
    stop = {"flag": False}

    def _sig(_a, _b):
        stop["flag"] = True

    _signal.signal(_signal.SIGTERM, _sig)
    _signal.signal(_signal.SIGINT, _sig)
    logger.info("v_meta_train_timer starting (interval=%ds enabled=%s)", interval, enabled)

    while not stop["flag"]:
        if not enabled:
            logger.info("V_META_TRAIN_ENABLED=0, skipping cycle")
        else:
            t0 = time.time()
            try:
                rc = main()
                logger.info("cycle done in %.1fs (rc=%d)", time.time() - t0, rc)
            except SystemExit as e:
                logger.warning("cycle exited %s", e.code)
            except Exception as e:
                logger.exception("cycle failed: %s", e)
                redis_url = os.environ.get("REDIS_URL", "")
                if redis_url:
                    try:
                        import redis as _redis
                        r = _redis.from_url(redis_url, socket_timeout=3.0)
                        r.set(
                            os.environ.get("V_META_METRICS_KEY", "metrics:v_meta_train:last"),
                            json.dumps({
                                "ts_ms": int(time.time() * 1000),
                                "status": "error",
                                "error": str(e)[:500],
                            }, separators=(",", ":")),
                        )
                    except Exception:
                        pass
        for _ in range(interval):
            if stop["flag"]:
                break
            time.sleep(1)
    logger.info("v_meta_train_timer stopped")
    return 0


if __name__ == "__main__":
    if "--loop" in sys.argv:
        sys.argv = [a for a in sys.argv if a != "--loop"]
        sys.exit(loop_main())
    sys.exit(main())
