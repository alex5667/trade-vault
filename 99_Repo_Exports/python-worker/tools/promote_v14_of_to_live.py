#!/usr/bin/env python3
"""Promote the v14_of GBDT challenger model to the MLScoringGate live path.

Workflow:
1. Read `cfg:ml_confirm:v14_of:gbdt_candidate` from Redis (written by
   `v14-of-train-timer` at every retrain).
2. Atomically copy the referenced `edge_stack_v14_of_challenger_<TS>.joblib`
   to the live path (env `ML_SCORER_V14_OF_LIVE_PATH`, default
   `/var/lib/trade/ml_models/scorer_v14_of/scorer_v14_of.joblib`).
3. Record the promotion in Redis at `metrics:promotion:v14_of:last` for audit.

The promotion is GUARDED:
  - Skip if candidate ROC-AUC < `MIN_ROC_AUC` (default 0.65).
  - Skip if candidate equals the currently-live run_id (idempotent).
  - Skip if mode != "SHADOW" (we only promote shadow-flagged candidates).

Usage:
  python -m tools.promote_v14_of_to_live              # one-shot promote
  python -m tools.promote_v14_of_to_live --dry-run    # log only
  python -m tools.promote_v14_of_to_live --force      # bypass guards (NOT recommended)

Env:
  REDIS_URL                       (default redis://redis-worker-1:6379/0)
  V14_OF_CANDIDATE_KEY            (default cfg:ml_confirm:v14_of:gbdt_candidate)
  ML_SCORER_V14_OF_LIVE_PATH      live target path
  V14_OF_PROMOTE_MIN_ROC_AUC      guard threshold (default 0.65)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import time
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("promote_v14_of")

DEFAULT_LIVE_PATH = "/var/lib/trade/ml_models/scorer_v14_of/scorer_v14_of.joblib"
DEFAULT_CANDIDATE_KEY = "cfg:ml_confirm:v14_of:gbdt_candidate"
DEFAULT_MIN_ROC_AUC = 0.65
PROMOTION_METRIC_KEY = "metrics:promotion:v14_of:last"


def _redis():
    import redis as _redis
    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = _redis.from_url(url, decode_responses=True, socket_connect_timeout=5)
    r.ping()
    return r


def _read_candidate(r: Any, key: str) -> dict | None:
    raw = r.get(key)
    if raw is None:
        log.warning("No candidate at %s — has v14-of-train-timer ever run?", key)
        return None
    try:
        return json.loads(raw)
    except Exception as e:
        log.error("Candidate at %s is not JSON: %s", key, e)
        return None


def _read_live_meta(live_path: str) -> dict | None:
    """If a sidecar `.meta.json` exists next to live model, return its content."""
    meta_path = live_path + ".meta.json"
    if not os.path.isfile(meta_path):
        return None
    try:
        with open(meta_path) as f:
            return json.load(f)
    except Exception:
        return None


def _write_live_meta(live_path: str, candidate: dict) -> None:
    meta_path = live_path + ".meta.json"
    tmp_path = meta_path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump({
            "promoted_at_ms": int(time.time() * 1000),
            "run_id": candidate.get("run_id"),
            "model_path_source": candidate.get("model_path"),
            "feature_schema_ver": candidate.get("feature_schema_ver"),
            "model_signature": candidate.get("model_signature"),
            "metrics": candidate.get("metrics"),
        }, f, indent=2)
    os.replace(tmp_path, meta_path)


def _atomic_copy(src: str, dst: str) -> None:
    """Copy src → dst.tmp → rename. Avoids partial reads during MLScoringGate
    hot-reload (which only sees the final atomic rename)."""
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    tmp = dst + ".tmp"
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--candidate-key", default=os.getenv("V14_OF_CANDIDATE_KEY", DEFAULT_CANDIDATE_KEY))
    ap.add_argument("--live-path", default=os.getenv("ML_SCORER_V14_OF_LIVE_PATH", DEFAULT_LIVE_PATH))
    ap.add_argument("--min-roc-auc", type=float,
                    default=float(os.getenv("V14_OF_PROMOTE_MIN_ROC_AUC", str(DEFAULT_MIN_ROC_AUC))))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="Bypass guards. Use only for one-shot bootstrap.")
    args = ap.parse_args()

    try:
        r = _redis()
    except Exception as e:
        log.error("Redis connect failed: %s", e)
        return 2

    cand = _read_candidate(r, args.candidate_key)
    if cand is None:
        return 1

    run_id = cand.get("run_id", "")
    src_path = cand.get("model_path", "")
    metrics = cand.get("metrics") or {}
    roc_auc = float(metrics.get("roc_auc_oof", 0.0) or 0.0)
    mode = str(cand.get("mode", "")).upper()

    log.info("Candidate: run_id=%s, ROC-AUC=%.4f, mode=%s, src=%s",
             run_id, roc_auc, mode, src_path)

    # Guards
    if not args.force:
        if roc_auc < args.min_roc_auc:
            log.warning("SKIP: ROC-AUC %.4f < %.4f", roc_auc, args.min_roc_auc)
            return 0
        if mode and mode != "SHADOW":
            log.warning("SKIP: candidate mode=%s != SHADOW", mode)
            return 0
        live_meta = _read_live_meta(args.live_path)
        if live_meta and live_meta.get("run_id") == run_id:
            log.info("SKIP: live already at run_id=%s (idempotent)", run_id)
            return 0

    if not src_path or not os.path.isfile(src_path):
        log.error("Candidate source file missing: %s", src_path)
        return 1

    if args.dry_run:
        log.info("[dry-run] would promote %s → %s", src_path, args.live_path)
        return 0

    try:
        _atomic_copy(src_path, args.live_path)
        _write_live_meta(args.live_path, cand)
    except Exception as e:
        log.error("Promotion copy failed: %s", e)
        return 2

    log.info("✅ Promoted %s → %s (run_id=%s, ROC-AUC=%.4f)",
             src_path, args.live_path, run_id, roc_auc)

    # Audit metric (Redis string, TTL 30 days)
    try:
        r.set(PROMOTION_METRIC_KEY, json.dumps({
            "promoted_at_ms": int(time.time() * 1000),
            "run_id": run_id,
            "roc_auc_oof": roc_auc,
            "live_path": args.live_path,
            "src_path": src_path,
        }), ex=30 * 86400)
    except Exception as e:
        log.warning("Failed to write audit metric %s: %s", PROMOTION_METRIC_KEY, e)

    return 0


if __name__ == "__main__":
    sys.exit(main())
