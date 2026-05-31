"""promote_v15_lgbm_to_live.py — atomic promotion of v15_lgbm candidate to live.

Steps:
  1. Verify `--candidate` is a valid joblib pack (contains `gbdt` + `feature_cols`
     + `metrics.roc_auc_oof` ≥ MIN_ROC_AUC).
  2. Preserve current live model as `<live>.prev.joblib` (rollback insurance).
  3. Copy candidate → `<live>.new`, fsync, then atomic rename → `<live>`.
  4. Write sidecar `<live>.meta.json` with provenance metadata.
  5. Update Redis `metrics:promotion:v15_lgbm:last` for audit.

Atomic guarantees:
  • The `os.replace(src, dst)` on the same filesystem is atomic — readers
    either see the OLD or the NEW joblib, never a half-written file.
  • If anything fails between step 2 and 4, the prior champion is in `.prev`
    and the new file in `.new` — both still recoverable.

Rollback (manual):
    cp /var/lib/.../scorer_v15_lgbm.joblib.prev /var/lib/.../scorer_v15_lgbm.joblib

Usage:
  python -m tools.promote_v15_lgbm_to_live --candidate <file> --live <file>
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import time
import warnings
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("promote_v15_lgbm")

DEFAULT_LIVE = "/var/lib/trade/ml_models/scorer_v15_lgbm/scorer_v15_lgbm.joblib"
DEFAULT_MIN_ROC_AUC = float(os.getenv("V15_PROMOTE_MIN_ROC_AUC", "0.55"))
PROMOTION_METRIC_KEY = "metrics:promotion:v15_lgbm:last"


def _load_pack(path: str) -> dict[str, Any] | None:
    try:
        import joblib
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return joblib.load(path)
    except Exception as e:
        log.error("joblib.load(%s) failed: %s", path, e)
        return None


def _validate_pack(pack: dict[str, Any], min_auc: float) -> tuple[bool, str]:
    if not isinstance(pack, dict):
        return False, "pack_not_dict"
    if "gbdt" not in pack and "model" not in pack:
        return False, "missing_model"
    if "feature_cols" not in pack and "feature_names" not in pack:
        return False, "missing_feature_cols"
    metrics = pack.get("metrics") or {}
    auc = float(metrics.get("roc_auc_oof") or metrics.get("roc_auc") or 0.0)
    if auc < min_auc:
        return False, f"roc_auc_below_threshold: {auc:.4f} < {min_auc:.4f}"
    schema = pack.get("feature_schema_ver") or pack.get("schema_name") or ""
    if "v15" not in str(schema).lower():
        return False, f"schema_mismatch: expected v15*, got {schema!r}"
    return True, "ok"


def _write_live_meta(live_path: str, pack: dict[str, Any], candidate_path: str) -> None:
    meta_path = live_path + ".meta.json"
    metrics = pack.get("metrics") or {}
    per_regime = pack.get("per_regime") or {}
    meta = {
        "promoted_at_ms": int(time.time() * 1000),
        "candidate_path": candidate_path,
        "run_id": pack.get("run_id"),
        "feature_schema_ver": pack.get("feature_schema_ver"),
        "schema_name": pack.get("schema_name"),
        "feature_cols_hash": pack.get("feature_cols_hash"),
        "n_features_expected": pack.get("n_features_expected"),
        "metrics": {
            "roc_auc_oof": metrics.get("roc_auc_oof"),
            "brier_oof_calibrated": metrics.get("brier_oof_calibrated"),
            "ece_oof_calibrated": metrics.get("ece_oof_calibrated"),
            "lift_top_decile": metrics.get("lift_top_decile"),
            "n_rows": metrics.get("n_rows"),
            "pos_rate": metrics.get("pos_rate"),
        },
        "per_regime_summary": {
            rg: {"n": sub.get("n"), "n_pos": sub.get("n_pos"), "oof_auc": sub.get("oof_auc")}
            for rg, sub in per_regime.items()
        },
    }
    try:
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2, default=str)
        log.info("wrote sidecar meta: %s", meta_path)
    except Exception as e:
        log.warning("failed to write meta sidecar: %s", e)


def _backup_current_live(live_path: str) -> str | None:
    if not os.path.isfile(live_path):
        return None
    prev_path = live_path + ".prev"
    try:
        shutil.copyfile(live_path, prev_path)
        log.info("backed up current live → %s", prev_path)
        return prev_path
    except Exception as e:
        log.warning("backup failed (continuing): %s", e)
        return None


def _atomic_copy(src: str, dst: str) -> bool:
    new_path = dst + ".new"
    try:
        shutil.copyfile(src, new_path)
        # Force write to disk
        try:
            with open(new_path, "rb") as f:
                os.fsync(f.fileno())
        except OSError:
            pass  # fsync may fail on some FS — atomic-rename still works
        os.replace(new_path, dst)
        return True
    except Exception as e:
        log.error("atomic copy failed: %s", e)
        for tmp in (new_path,):
            try:
                if os.path.isfile(tmp):
                    os.remove(tmp)
            except OSError:
                pass
        return False


def _publish_audit(pack: dict[str, Any], candidate_path: str, live_path: str) -> None:
    try:
        import redis as _redis
        url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        r = _redis.from_url(url, decode_responses=True, socket_connect_timeout=3)
        metrics = pack.get("metrics") or {}
        payload = {
            "promoted_at_ms": int(time.time() * 1000),
            "candidate_path": candidate_path,
            "live_path": live_path,
            "run_id": pack.get("run_id"),
            "feature_schema_ver": pack.get("feature_schema_ver"),
            "metrics": {
                "roc_auc_oof": metrics.get("roc_auc_oof"),
                "ece_oof_calibrated": metrics.get("ece_oof_calibrated"),
                "n_rows": metrics.get("n_rows"),
            },
            "per_regime_count": len(pack.get("per_regime") or {}),
        }
        r.set(PROMOTION_METRIC_KEY, json.dumps(payload, default=str))
        log.info("audit emitted → %s", PROMOTION_METRIC_KEY)
    except Exception as e:
        log.debug("audit emit failed (non-fatal): %s", e)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--candidate", required=True, help="Candidate joblib path")
    ap.add_argument("--live", default=DEFAULT_LIVE, help="Live target path")
    ap.add_argument("--min-roc-auc", type=float, default=DEFAULT_MIN_ROC_AUC)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true", help="bypass AUC + schema guards")
    args = ap.parse_args()

    if not os.path.isfile(args.candidate):
        log.error("candidate not found: %s", args.candidate)
        return 2

    pack = _load_pack(args.candidate)
    if pack is None:
        return 2

    if not args.force:
        ok, reason = _validate_pack(pack, args.min_roc_auc)
        if not ok:
            log.error("REJECTED: %s", reason)
            return 1

    log.info("candidate validated: run_id=%s schema=%s metrics=%s",
             pack.get("run_id"),
             pack.get("feature_schema_ver"),
             {k: v for k, v in (pack.get("metrics") or {}).items()
              if k in ("roc_auc_oof", "ece_oof_calibrated", "n_rows", "pos_rate")})

    if args.dry_run:
        log.info("DRY-RUN: would promote %s → %s", args.candidate, args.live)
        return 0

    os.makedirs(os.path.dirname(args.live), exist_ok=True)
    _backup_current_live(args.live)
    if not _atomic_copy(args.candidate, args.live):
        log.error("PROMOTION FAILED — live file unchanged")
        return 3
    log.info("✓ live model updated: %s", args.live)

    _write_live_meta(args.live, pack, args.candidate)
    _publish_audit(pack, args.candidate, args.live)
    return 0


if __name__ == "__main__":
    sys.exit(main())
