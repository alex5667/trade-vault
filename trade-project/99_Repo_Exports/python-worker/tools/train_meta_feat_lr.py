"""Train an LR on the small Meta-path feature subset (META_FEAT_V*_OF_COLS).

This is the Meta-path trainer — separate from `nightly_v14_of_train_bundle.py`
which trains on the FULL feature_registry schema (359/515 keys, fed into the
ml_confirm gate / edge_stack). This script trains on the small ~236/245-col
subset that `core.meta_features_v{14,15}_of.build_meta_features_v*` produces,
which is what `of_confirm_engine` Meta path expects:

  - schema_name="meta_feat_v14_of"  cols=META_FEAT_V14_OF_COLS  schema_version=14
  - schema_name="meta_feat_v15_of"  cols=META_FEAT_V15_OF_COLS  schema_version=15

The schema-guard at `of_confirm_engine.py:5181` matches model artifact name +
version + hash against the registry (`SCHEMAS` dict), so once this artifact
is produced and `META_MODEL_PATH` (or cfg) points at it, the Meta path can
serve in ENFORCE mode without falling back to v1 features.

Data source (reuses bundle output):
  The v14/v15 nightly bundles build `ml_dataset_v{14,15}.jsonl` with rows of
  {indicators: {…}, y_edge_cost_aware: 0|1}. We re-vectorize via
  build_meta_features_v*_of(evidence={}, indicators=row.indicators) so the
  serving and training feature paths share a single implementation
  (Train==Serve parity).

Usage (CLI):
  python -m tools.train_meta_feat_lr --schema v14_of \
      --dataset /var/lib/trade/of_reports/v14_of_train_work/ml_dataset_v14.jsonl \
      --out /var/lib/trade/of_reports/models/

  python -m tools.train_meta_feat_lr --schema v15_of \
      --dataset /var/lib/trade/of_reports/v15_of_train_work/ml_dataset_v15.jsonl

  python -m tools.train_meta_feat_lr --dry-run --schema v14_of
      (skips training; reports column count + first dataset row for sanity)

Exit codes:
  0 = trained + written model file
  1 = error (missing dataset, sklearn import failure, etc.)
  2 = insufficient data (<100 rows or <10 positives)
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
from typing import Any, Callable

logger = logging.getLogger("train_meta_feat_lr")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _select_schema(schema_ver: str) -> dict[str, Any]:
    if schema_ver == "v14_of":
        from core.meta_features_v14_of import (
            META_FEAT_V14_OF_COLS as COLS,
            META_FEAT_V14_OF_HASH as HASH,
            META_FEAT_V14_OF_NAME as NAME,
            META_FEAT_V14_OF_VERSION as VERSION,
            build_meta_features_v14_of as BUILDER,
        )
    elif schema_ver == "v15_of":
        from core.meta_features_v15_of import (
            META_FEAT_V15_OF_COLS as COLS,
            META_FEAT_V15_OF_HASH as HASH,
            META_FEAT_V15_OF_NAME as NAME,
            META_FEAT_V15_OF_VERSION as VERSION,
            build_meta_features_v15_of as BUILDER,
        )
    else:
        raise SystemExit(f"unknown --schema {schema_ver!r} (use v14_of or v15_of)")
    return {
        "schema_ver": schema_ver,
        "name": NAME,
        "version": VERSION,
        "hash": HASH,
        "cols": list(COLS),
        "builder": BUILDER,
    }


def _load_dataset(path: Path, builder: Callable, cols: list[str], y_col: str):
    """Read JSONL → (X, y, n_skipped). Vectorize via meta builder for Train==Serve."""
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

    X_rows: list[list[float]] = []
    y_rows: list[int] = []
    n_skipped = 0
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                n_skipped += 1
                continue
            ind = rec.get("indicators") or {}
            if not isinstance(ind, dict):
                n_skipped += 1
                continue
            # Build meta-features via the SAME function used at serving time.
            feat, _missing = builder(evidence={}, indicators=ind)
            X_rows.append([_ff(feat.get(k)) for k in cols])
            y_val = rec.get(y_col, rec.get("y_edge", 0)) or 0
            y_rows.append(int(y_val))

    X = np.array(X_rows, dtype=np.float64)
    y = np.array(y_rows, dtype=np.int64)
    return X, y, n_skipped


def _train_lr_cv(X, y) -> tuple[Any, dict[str, float]]:
    """5-fold StratifiedKFold CV; return final LR fitted on all data + mean metrics."""
    import numpy as np
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import (average_precision_score, brier_score_loss,
                                 log_loss, roc_auc_score)
    from sklearn.model_selection import StratifiedKFold
    from sklearn.preprocessing import StandardScaler

    auc_l: list[float] = []
    ap_l: list[float] = []
    brier_l: list[float] = []
    ll_l: list[float] = []

    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    for tr, te in skf.split(X, y):
        sc = StandardScaler().fit(X[tr])
        Xtr, Xte = sc.transform(X[tr]), sc.transform(X[te])
        lr = LogisticRegression(max_iter=2000, C=0.1, solver="lbfgs")
        lr.fit(Xtr, y[tr])
        p = lr.predict_proba(Xte)[:, 1]
        try:
            auc_l.append(float(roc_auc_score(y[te], p)))
        except Exception:
            pass
        try:
            ap_l.append(float(average_precision_score(y[te], p)))
        except Exception:
            pass
        try:
            brier_l.append(float(brier_score_loss(y[te], p)))
        except Exception:
            pass
        try:
            ll_l.append(float(log_loss(y[te], p, labels=[0, 1])))
        except Exception:
            pass

    # Final fit on full data.
    sc_full = StandardScaler().fit(X)
    Xs = sc_full.transform(X)
    lr_full = LogisticRegression(max_iter=2000, C=0.1, solver="lbfgs")
    lr_full.fit(Xs, y)

    def _mean(L):
        return float(sum(L) / len(L)) if L else 0.0

    metrics = {
        "auc_mean": _mean(auc_l),
        "pr_auc_mean": _mean(ap_l),
        "brier_mean": _mean(brier_l),
        "log_loss_mean": _mean(ll_l),
        "n_rows": int(X.shape[0]),
        "n_pos": int(y.sum()),
        "n_features": int(X.shape[1]),
    }

    # Pack robust_scaler with center=mean, scale=std (StandardScaler) — keeps
    # serve-time transform compatible with MetaModelLR.robust_scaler format.
    rs_params: dict[str, dict[str, float]] = {}
    means = sc_full.mean_
    scales = sc_full.scale_
    return (lr_full, means, scales, metrics), metrics


def _build_pack(*, schema: dict[str, Any], lr, means, scales, metrics: dict[str, float],
                ts_str: str) -> dict[str, Any]:
    """Build MetaModelLR-compatible JSON pack (matches core/meta_model_lr.py:load)."""
    cols = schema["cols"]
    rs_dict: dict[str, dict[str, float]] = {}
    for i, k in enumerate(cols):
        rs_dict[k] = {"center": float(means[i]), "scale": float(scales[i]) or 1.0}

    pack: dict[str, Any] = {
        "features": cols,
        "intercept": float(lr.intercept_[0]),
        "coef": [float(c) for c in lr.coef_[0]],
        "threshold": 0.5,
        "transforms": {},  # Meta path applies builder's transforms upstream
        "robust_scaler": rs_dict,
        "schema_name": schema["name"],
        # schema_version on the model JSON = feature_schema_version.
        # MetaModelLR.load reads `schema_version or feature_schema_version`,
        # and of_confirm_engine schema-guard compares it to local registry.
        "schema_version": int(schema["version"]),
        "schema_hash": str(schema["hash"]),
        "feature_cols_hash": hashlib.sha256(",".join(cols).encode("utf-8")).hexdigest()[:16],
        "created_ms": int(time.time() * 1000),
        "model_signature": "",
        "kind": "meta_lr",
        "run_id": f"{schema['name']}_{ts_str}",
        "feature_schema_ver": schema["schema_ver"],
        "feature_schema_version": int(schema["version"]),
        "metrics": metrics,
    }
    # Stable signature over canonical content (excluding signature itself).
    sig_in = dict(pack)
    sig_in.pop("model_signature", None)
    pack["model_signature"] = hashlib.sha256(
        json.dumps(sig_in, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:16]
    return pack


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="v14_of", choices=["v14_of", "v15_of"])
    ap.add_argument(
        "--dataset",
        default=os.getenv("META_LR_DATASET_PATH", ""),
        help="JSONL dataset with {indicators, y_edge_cost_aware}. "
             "Defaults to /var/lib/trade/of_reports/v{14|15}_of_train_work/ml_dataset_v{14|15}.jsonl",
    )
    ap.add_argument(
        "--out",
        default=os.getenv("META_LR_OUT_DIR", "/var/lib/trade/of_reports/models"),
        help="Output directory (created if missing).",
    )
    ap.add_argument("--y-col", default=os.getenv("META_LR_Y_COL", "y_edge_cost_aware"))
    ap.add_argument("--min-rows", type=int, default=int(os.getenv("META_LR_MIN_ROWS", "100")))
    ap.add_argument("--min-pos", type=int, default=int(os.getenv("META_LR_MIN_POS", "10")))
    ap.add_argument("--dry-run", action="store_true",
                    help="Inspect schema + first row of dataset; do not train.")
    args = ap.parse_args()

    schema = _select_schema(args.schema)
    logger.info("schema=%s name=%s version=%d hash=%s n_cols=%d",
                args.schema, schema["name"], schema["version"], schema["hash"][:12], len(schema["cols"]))

    if not args.dataset:
        # Pick the default dataset path keyed by schema.
        if args.schema == "v14_of":
            args.dataset = "/var/lib/trade/of_reports/v14_of_train_work/ml_dataset_v14.jsonl"
        else:
            args.dataset = "/var/lib/trade/of_reports/v15_of_train_work/ml_dataset_v15.jsonl"

    ds_path = Path(args.dataset)
    if not ds_path.exists():
        logger.error("dataset not found: %s", ds_path)
        if args.dry_run:
            # Don't fail the dry-run on missing dataset — schema-check is enough.
            logger.info("dry-run: skipping dataset load")
            return 0
        return 1

    if args.dry_run:
        # Peek the first row + report
        with ds_path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                ind = rec.get("indicators") or {}
                feat, missing = schema["builder"](evidence={}, indicators=ind)
                logger.info("first row: indicators_keys=%d feat=%d missing=%d",
                            len(ind), len(feat), len(missing))
                break
        return 0

    logger.info("loading dataset %s", ds_path)
    X, y, n_skipped = _load_dataset(ds_path, schema["builder"], schema["cols"], args.y_col)
    logger.info("loaded: rows=%d pos=%d skipped=%d", X.shape[0], int(y.sum()), n_skipped)
    if X.shape[0] < args.min_rows or int(y.sum()) < args.min_pos:
        logger.error("insufficient data: rows=%d pos=%d (need ≥%d / ≥%d)",
                     X.shape[0], int(y.sum()), args.min_rows, args.min_pos)
        return 2

    logger.info("training 5-fold CV LR + final fit...")
    (lr_full, means, scales, metrics), _ = _train_lr_cv(X, y)
    logger.info("metrics: %s", metrics)

    ts_str = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    pack = _build_pack(schema=schema, lr=lr_full, means=means, scales=scales,
                       metrics=metrics, ts_str=ts_str)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{schema['name']}_baseline_{ts_str}.json"
    out_path.write_text(json.dumps(pack, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    logger.info("wrote %s", out_path)
    print(json.dumps({"status": "ok", "path": str(out_path), "metrics": metrics,
                      "schema_name": schema["name"], "schema_hash": schema["hash"][:16]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
