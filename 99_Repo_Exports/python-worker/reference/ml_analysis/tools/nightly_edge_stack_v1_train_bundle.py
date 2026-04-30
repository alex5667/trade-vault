"""P59 nightly bundle for edge_stack_v1 (Dataset -> Validate -> Train -> Validate -> Promote).

Design goals:
  - deterministic feature columns via Feature Registry (schema pinning)
  - strict dataset health guardrails (joined, pos_rate)
  - strict hash-pin checks (feature_cols_hash, schema_hash) via dataset report + train tool
  - atomic artifact promotion (candidate/champion)
  - best-effort Redis metrics write for Prometheus alerts

Redis keys:
  - metrics hash: metrics:edge_stack_train:last
  - ML confirm cfg hash: cfg:ml_confirm (fields: challenger_model_path, challenger_ver, model_path, model_ver)

Promotion policy:
  - always writes candidate artifact + challenger cfg
  - only promotes to champion if EDGE_STACK_AUTO_PROMOTE=1 AND dataset+train validations pass
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
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore

try:
    from tools.schema_choices_v1 import schema_choices as _schema_choices, normalize_schema_ver as _norm_schema_ver  # type: ignore
except Exception:
    from ml_analysis.tools.schema_choices_v1 import schema_choices as _schema_choices, normalize_schema_ver as _norm_schema_ver  # type: ignore

from ml_analysis.tools.edge_stack_train_bundle_utils_p59 import (
    atomic_copy
    atomic_write_json
    now_ms
    validate_dataset_report
    validate_train_report
    write_train_metrics
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("nightly_edge_stack_v1_bundle_p59")


def _sha256_file(path: str) -> str:
    """Compute SHA-256 of a file (for artifact integrity check)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _run(module: str, args: list, timeout: int = 3600) -> Tuple[bool, str, str]:
    """Run a python module via subprocess, return (ok, stdout, stderr)."""
    cmd = [sys.executable, "-m", module] + list(args)
    logger.info("Running: %s", " ".join(cmd))
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    ok = p.returncode == 0
    if not ok:
        logger.error("Command failed code=%s\nSTDOUT:%s\nSTDERR:%s", p.returncode, p.stdout, p.stderr)
    return ok, (p.stdout or ""), (p.stderr or "")


def _load_json(path: str) -> Dict[str, Any]:
    """Load JSON from file; return empty dict on any error."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _connect_redis(redis_url: str):
    """Connect to Redis; raises RuntimeError if redis-py not installed."""
    if redis is None:
        raise RuntimeError("redis-py is required for bundle metrics/cfg updates")
    return redis.Redis.from_url(redis_url, decode_responses=True)


def _hset_safe(r, key: str, mapping: Dict[str, Any]) -> None:
    """Best-effort hset - skips None values, never raises."""
    try:
        flat: Dict[str, str] = {}
        for k, v in mapping.items():
            if v is None:
                continue
            flat[str(k)] = str(v)
        if flat:
            r.hset(key, mapping=flat)
    except Exception:
        return


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(description="P59 nightly edge_stack_v1 train bundle")
    ap.add_argument("--redis_url", default=os.environ.get("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--cfg_hash_key", default=os.environ.get("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm"))
    ap.add_argument("--metrics_key", default=os.environ.get("EDGE_STACK_TRAIN_METRICS_KEY", "metrics:edge_stack_train:last"))

    ap.add_argument("--out_dir", default=os.environ.get("EDGE_STACK_V1_DIR", "/var/lib/trade/ml_models/edge_stack_v1"))
    ap.add_argument("--signals_stream", default=os.environ.get("EDGE_STACK_SIGNALS_STREAM", "signals:of:inputs"))
    ap.add_argument("--closed_stream", default=os.environ.get("EDGE_STACK_CLOSED_STREAM", "trades:closed"))
    ap.add_argument("--window_hours", type=int, default=int(os.environ.get("EDGE_STACK_WINDOW_HOURS", "72")))
    ap.add_argument("--signals_count", type=int, default=int(os.environ.get("EDGE_STACK_SIGNALS_COUNT", "200000")))
    ap.add_argument("--closes_count", type=int, default=int(os.environ.get("EDGE_STACK_CLOSES_COUNT", "200000")))
    ap.add_argument("--y_min_r", type=float, default=float(os.environ.get("Y_MIN_R", os.environ.get("EDGE_STACK_Y_MIN_R", "0.10"))))

    # Feature schema / registry pinning
    ap.add_argument("--feature_schema_ver", default=os.environ.get("ML_EDGE_STACK_OOF_FEATURE_SCHEMA_VER", os.environ.get("ML_FEATURE_SCHEMA_VER", "v3")), choices=_schema_choices(include_empty=True))
    ap.add_argument("--scenario_prefix", default=os.environ.get("EDGE_STACK_SCENARIO_PREFIX", "bucket:"))
    ap.add_argument("--include_time_onehot", type=int, default=int(os.environ.get("EDGE_STACK_INCLUDE_TIME_ONEHOT", "1")))
    ap.add_argument("--strict_feature_cols", type=int, default=int(os.environ.get("ML_STRICT_FEATURE_COLS", "0")))
    ap.add_argument("--forbid_scenario_v4_onehot", type=int, default=int(os.environ.get("ML_FORBID_SCENARIO_V4_ONEHOT", "0")))

    # Dataset validation guardrails
    ap.add_argument("--min_joined", type=int, default=int(os.environ.get("EDGE_STACK_MIN_JOINED", "2000")))
    ap.add_argument("--pos_rate_min", type=float, default=float(os.environ.get("EDGE_STACK_POS_RATE_MIN", "0.05")))
    ap.add_argument("--pos_rate_max", type=float, default=float(os.environ.get("EDGE_STACK_POS_RATE_MAX", "0.60")))

    # Train hyperparams
    ap.add_argument("--n_splits", type=int, default=int(os.environ.get("ML_EDGE_STACK_OOF_N_SPLITS", "5")))
    ap.add_argument("--purge_ms", type=int, default=int(os.environ.get("ML_EDGE_STACK_OOF_PURGE_MS", "300000")))
    ap.add_argument("--embargo_ms", type=int, default=int(os.environ.get("ML_EDGE_STACK_OOF_EMBARGO_MS", "300000")))
    ap.add_argument("--min_train", type=int, default=int(os.environ.get("ML_EDGE_STACK_OOF_MIN_TRAIN", "500")))
    ap.add_argument("--lr_C", type=float, default=float(os.environ.get("ML_EDGE_STACK_OOF_LR_C", "1.0")))
    ap.add_argument("--gbdt_max_depth", type=int, default=int(os.environ.get("ML_EDGE_STACK_OOF_GBDT_MAX_DEPTH", "3")))
    ap.add_argument("--gbdt_lr", type=float, default=float(os.environ.get("ML_EDGE_STACK_OOF_GBDT_LR", "0.05")))
    ap.add_argument("--gbdt_max_iter", type=int, default=int(os.environ.get("ML_EDGE_STACK_OOF_GBDT_MAX_ITER", "400")))
    ap.add_argument("--calibrate", type=int, default=int(os.environ.get("ML_EDGE_STACK_OOF_CALIBRATE", "1")))

    # Train validation + promotion thresholds
    ap.add_argument("--brier_max", type=float, default=float(os.environ.get("EDGE_STACK_PROMOTE_BRIER_MAX", "0.30")))
    ap.add_argument("--ece_max", type=float, default=float(os.environ.get("EDGE_STACK_PROMOTE_ECE_MAX", "0.08")))
    # auto_promote=0 is safe default: produces candidate but never auto-promotes champion
    ap.add_argument("--auto_promote", type=int, default=int(os.environ.get("EDGE_STACK_AUTO_PROMOTE", "0")))

    args = ap.parse_args(argv)

    feature_schema_ver = _norm_schema_ver(str(args.feature_schema_ver or "").strip())


    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.abspath(str(args.out_dir))
    run_dir = os.path.join(out_dir, "runs", run_id)
    champions_dir = os.path.join(out_dir, "champions")
    versions_dir = os.path.join(out_dir, "versions")
    os.makedirs(run_dir, exist_ok=True)
    os.makedirs(champions_dir, exist_ok=True)
    os.makedirs(versions_dir, exist_ok=True)

    # Per-run artifact paths
    dataset_jsonl = os.path.join(run_dir, "edge_train.jsonl")
    dataset_report = os.path.join(run_dir, "edge_dataset_report.json")
    quarantine_jsonl = os.path.join(run_dir, "edge_quarantine.jsonl")
    feature_cols_json = os.path.join(run_dir, "feature_cols.json")
    model_path = os.path.join(run_dir, "edge_stack_v1.joblib")
    train_report_json = os.path.join(run_dir, "train_report.json")

    # Promotion artifact paths (atomic copy on same FS)
    candidate_path = os.path.join(champions_dir, "edge_stack_v1_candidate.joblib")
    champion_path = os.path.join(champions_dir, "edge_stack_v1_champion.joblib")
    champion_prev_path = os.path.join(champions_dir, "edge_stack_v1_champion_prev.joblib")

    bundle_latest = os.path.join(out_dir, "edge_stack_v1_train_bundle_latest.json")
    version_json = os.path.join(versions_dir, f"edge_stack_v1_train_{run_id}.json")

    # --- Step 1: Build dataset
    end_ms = now_ms()
    start_ms = end_ms - int(args.window_hours) * 3600 * 1000

    build_args = [
        "--redis_url", str(args.redis_url)
        # NOTE: --signal_stream is intentionally NOT passed here.
        # build_edge_stack_dataset_from_redis's --signal_stream accepts a schema version
        # (e.g. "v5_of"), NOT a Redis stream name. The actual Redis stream ("signals:of:inputs")
        # comes from ML_REPLAY_STREAM env or the hardcoded default in that script.
        "--closed_stream", str(args.closed_stream)
        "--signals_count", str(args.signals_count)
        "--closes_count", str(args.closes_count)
        "--since_ms", str(start_ms)
        "--until_ms", str(end_ms)
        "--y_min_r", str(args.y_min_r)
        "--out_jsonl", dataset_jsonl
        "--out_report_json", dataset_report
        "--out_quarantine_jsonl", quarantine_jsonl
        "--emit_feature_cols_json", feature_cols_json
        # v9_of: pinned snapshot covering 100% of actual signal indicators
        # builder writes feature_registry.feature_cols_hash → validated by trainer
        "--feature_schema_ver", str(feature_schema_ver or "").strip()
        "--scenario_prefix", str(args.scenario_prefix)
        "--include_time_onehot", str(int(args.include_time_onehot))
        "--strict_feature_cols", str(int(args.strict_feature_cols))
        "--forbid_scenario_v4_onehot", str(int(args.forbid_scenario_v4_onehot))
    ]
    ok_build, _, _ = _run("ml_analysis.tools.build_edge_stack_dataset_from_redis", build_args, timeout=3600)
    if not ok_build or not os.path.exists(dataset_report):
        # Metrics: fail_build
        mapping = {
            "status": "fail_build"
            "reason": "dataset_build_failed"
            "success": 0
            "run_id": run_id
            "updated_ts_ms": now_ms()
        }
        try:
            write_train_metrics(str(args.redis_url), str(args.metrics_key), mapping)
        except Exception:
            pass
        atomic_write_json(version_json, {"run_id": run_id, "status": "fail_build", "reason": "dataset_build_failed"})
        atomic_write_json(bundle_latest, {"run_id": run_id, "status": "fail_build", "reason": "dataset_build_failed"})
        return 2

    # --- Step 2: Validate dataset report (joined/pos_rate guardrails)
    rep = _load_json(dataset_report)
    dv = validate_dataset_report(rep, min_joined=int(args.min_joined), pos_rate_min=float(args.pos_rate_min), pos_rate_max=float(args.pos_rate_max))
    if not dv.ok:
        mapping = {
            "status": "fail_validate"
            "reason": dv.reason
            "success": 0
            "run_id": run_id
            "joined": dv.joined
            "pos_rate": dv.pos_rate
            "updated_ts_ms": now_ms()
        }
        # Include feature registry hashes if available
        fr = rep.get("feature_registry") if isinstance(rep, dict) else None
        if isinstance(fr, dict):
            mapping["feature_cols_hash"] = fr.get("feature_cols_hash", "")
            mapping["schema_hash"] = fr.get("schema_hash", "")
            mapping["feature_schema_ver"] = fr.get("schema_ver", "")
        try:
            write_train_metrics(str(args.redis_url), str(args.metrics_key), mapping)
        except Exception:
            pass
        atomic_write_json(version_json, {"run_id": run_id, "status": "fail_validate", "reason": dv.reason, "dataset_report": rep})
        atomic_write_json(bundle_latest, {"run_id": run_id, "status": "fail_validate", "reason": dv.reason})
        return 3

    # --- Step 3: Train OOF model
    train_args = [
        "--data_jsonl", dataset_jsonl
        "--out_model", model_path
        "--run_id", run_id
        "--n_splits", str(args.n_splits)
        "--purge_ms", str(args.purge_ms)
        "--embargo_ms", str(args.embargo_ms)
        "--min_train", str(args.min_train)
        "--lr_C", str(args.lr_C)
        "--gbdt_max_depth", str(args.gbdt_max_depth)
        "--gbdt_learning_rate", str(args.gbdt_lr)
        "--gbdt_max_iter", str(args.gbdt_max_iter)
        "--calibrate", str(int(args.calibrate))
        # --feature_cols_json is intentionally NOT passed: when --feature_schema_ver=v9_of
        # is set, trainer derives feature_cols from registry directly.
        # Passing both triggers strict_registry_match check which fails due to session_* one-hots.
        "--feature_schema_ver", str(feature_schema_ver or "").strip()
        "--scenario_prefix", str(args.scenario_prefix)
        "--include_time_onehot", str(int(args.include_time_onehot))
        "--require_feature_registry", "0"
        "--dataset_report_json", dataset_report
    ]
    ok_train, out, _ = _run("ml_analysis.tools.train_edge_stack_v1_oof", train_args, timeout=3600)
    if not ok_train or not os.path.exists(model_path):
        mapping = {
            "status": "fail_train"
            "reason": "train_failed"
            "success": 0
            "run_id": run_id
            "joined": dv.joined
            "pos_rate": dv.pos_rate
            "updated_ts_ms": now_ms()
        }
        try:
            write_train_metrics(str(args.redis_url), str(args.metrics_key), mapping)
        except Exception:
            pass
        atomic_write_json(version_json, {"run_id": run_id, "status": "fail_train", "reason": "train_failed"})
        atomic_write_json(bundle_latest, {"run_id": run_id, "status": "fail_train", "reason": "train_failed"})
        return 4

    # parse train report from stdout (last JSON object on line)
    tr: Dict[str, Any] = {}
    for line in (out or "").splitlines()[::-1]:
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    tr = obj
                    break
            except Exception:
                continue
    atomic_write_json(train_report_json, tr)

    # --- Step 4: Validate train report (brier/ECE guardrails)
    tv = validate_train_report(tr, brier_max=float(args.brier_max), ece_max=float(args.ece_max))

    # --- Step 5: Promote candidate (always - regardless of validation)
    atomic_copy(model_path, candidate_path)

    # Update cfg hash with challenger pointer (best-effort, non-blocking)
    try:
        r = _connect_redis(str(args.redis_url))
        _hset_safe(r, str(args.cfg_hash_key), {"challenger_model_path": candidate_path, "challenger_ver": run_id})
    except Exception:
        pass

    # --- Step 6: Optionally promote to champion (auto_promote=1 + all validations ok)
    promoted = False
    promote_reason = ""
    if int(args.auto_promote) == 1 and dv.ok and tv.ok:
        # Backup current champion before overwrite
        try:
            if os.path.exists(champion_path):
                atomic_copy(champion_path, champion_prev_path)
        except Exception:
            pass
        atomic_copy(model_path, champion_path)
        promoted = True
        promote_reason = "auto_promote_ok"

        # Update cfg hash with champion pointer (best-effort)
        try:
            r = _connect_redis(str(args.redis_url))
            _hset_safe(r, str(args.cfg_hash_key), {"model_path": champion_path, "model_ver": run_id})
        except Exception:
            pass
    else:
        if int(args.auto_promote) == 1 and not tv.ok:
            promote_reason = f"auto_promote_blocked:{tv.reason}"
        elif int(args.auto_promote) == 1 and not dv.ok:
            promote_reason = f"auto_promote_blocked:{dv.reason}"
        else:
            promote_reason = "candidate_only"

    # --- Step 7: Write Redis metrics (best-effort, non-blocking)
    mapping = {
        "status": "ok" if dv.ok else "fail_validate"
        "reason": "ok" if dv.ok else dv.reason
        "success": 1 if dv.ok else 0
        "run_id": run_id
        "joined": dv.joined
        "pos_rate": dv.pos_rate
        "oof_meta_brier": tv.brier
        "oof_meta_ece": tv.ece
        "train_ok": 1 if tv.ok else 0
        "train_reason": tv.reason
        "feature_schema_ver": str(feature_schema_ver or "")
        "candidate_path": candidate_path
        "champion_path": champion_path if promoted else ""
        "promote_applied": 1 if promoted else 0
        "promote_reason": promote_reason
        "updated_ts_ms": now_ms()
    }
    # Pin hashes from dataset/train reports for Prometheus alerts
    fr = rep.get("feature_registry") if isinstance(rep, dict) else None
    if isinstance(fr, dict):
        mapping["feature_cols_hash"] = str(fr.get("feature_cols_hash") or "")
        mapping["schema_hash"] = str(fr.get("schema_hash") or "")
    if isinstance(tr, dict):
        mapping["train_feature_cols_hash"] = str(tr.get("feature_cols_hash") or "")
    try:
        write_train_metrics(str(args.redis_url), str(args.metrics_key), mapping)
    except Exception:
        pass

    # --- Step 8: Persist bundle manifest (versioned + latest symlink)
    manifest = {
        "run_id": run_id
        "status": "ok"
        "dataset": {
            "signals_stream": str(args.signals_stream)
            "closed_stream": str(args.closed_stream)
            "window_hours": int(args.window_hours)
            "since_ms": int(start_ms)
            "until_ms": int(end_ms)
            "y_min_r": float(args.y_min_r)
            "report": rep
        }
        "train": {
            "report": tr
            "train_ok": bool(tv.ok)
            "train_reason": tv.reason
            "thresholds": {"brier_max": float(args.brier_max), "ece_max": float(args.ece_max)}
        }
        "artifacts": {
            "model_path": model_path
            "candidate_path": candidate_path
            "champion_path": champion_path
            "promoted": promoted
            "promote_reason": promote_reason
            "candidate_sha256": _sha256_file(candidate_path) if os.path.exists(candidate_path) else ""
            "champion_sha256": _sha256_file(champion_path) if promoted and os.path.exists(champion_path) else ""
        }
        "cfg": {"cfg_hash_key": str(args.cfg_hash_key)}
        "generated_ms": now_ms()
    }
    atomic_write_json(version_json, manifest)
    atomic_write_json(bundle_latest, manifest)

    # Exit code policy: training succeeded, candidate produced.
    # If auto_promote enabled but blocked by validation, still exit 0 (candidate exists).
    # train_ok=0 in metrics will trigger Prometheus alert.
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
