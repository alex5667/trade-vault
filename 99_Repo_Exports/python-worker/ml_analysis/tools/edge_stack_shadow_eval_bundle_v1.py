#!/usr/bin/env python3
from __future__ import annotations
"""P60 Edge Stack Shadow Eval Bundle.

Runs nightly:
  1) builds a dataset (last N hours) using tools.build_edge_stack_dataset_from_redis,
  2) loads champion + candidate configs from Redis,
  3) evaluates both models on the same dataset,
  4) computes shadow metrics (Brier/ECE/Precision@top5/Expectancy@top5),
  5) writes:
     - status file (for edge_stack_shadow_status_exporter_v1)
     - Redis hash metrics: metrics:edge_stack_shadow:last
  6) optionally promotes candidate -> champion if guarded.

Keys (defaults):
  - cfg:ml_confirm:edge_stack_v1:champion
  - cfg:ml_confirm:edge_stack_v1:candidate

This tool is deterministic (no randomness) and is safe to run multiple times.

Usage:
  python -m ml_analysis.tools.edge_stack_shadow_eval_bundle_v1 [--window_hours 24] [--auto_promote_guarded 0]
"""


import argparse
import json
import os
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    import numpy as np  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit(f"numpy is required: {e}")

try:
    import redis  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit(f"redis-py is required: {e}")

try:
    import joblib  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit(f"joblib is required: {e}")

# Import champion config validator
try:
    from core.champion_cfg_validator import validate_champion_cfg
except ImportError:
    validate_champion_cfg = None  # type: ignore

# Import shadow metrics helpers
from ml_analysis.tools.edge_stack_shadow_metrics_p60 import calculate_shadow_metrics, check_promotion_guard

# Import P59 bundle utilities (atomic write, now_ms, metrics writer)
from ml_analysis.tools.edge_stack_train_bundle_utils_p59 import (
    atomic_copy,
    atomic_write_json,
    now_ms,
    write_train_metrics,
)


def _read_jsonl(path: str) -> List[Dict[str, Any]]:
    """Read a JSONL file and return list of dicts."""
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                d = json.loads(s)
            except Exception:
                continue
            if isinstance(d, dict):
                out.append(d)
    return out


def _f(x: Any, d: float = 0.0) -> float:
    """Safe float conversion, NaN-safe."""
    try:
        v = float(x)
        return v if v == v else float(d)
    except Exception:
        return float(d)


def _i(x: Any, d: int = 0) -> int:
    """Safe int conversion."""
    try:
        return int(x)
    except Exception:
        return int(d)


def _load_cfg(r: redis.Redis, key: str) -> Optional[Dict[str, Any]]:
    """Load JSON config from Redis key."""
    raw = r.get(key)
    if not raw:
        return None
    try:
        obj = json.loads(raw)
    except Exception:
        return None
    if not isinstance(obj, dict) or not obj:
        return None
    return obj


def _predict_pack_p(model_pack: Dict[str, Any], row: Dict[str, Any]) -> Tuple[float, int]:
    """Return (p_edge_raw, missing_count).

    Uses the standard edge_stack_v1 pack format: lr + gbdt + meta stacking.
    missing_count counts how many numeric f_ keys were absent from indicators.
    """
    # Import build_feature_row lazily to avoid circular deps at module load time
    from ml_analysis.tools.train_edge_stack_v1_oof import build_feature_row  # type: ignore

    indicators = row.get("indicators") if isinstance(row.get("indicators"), dict) else {}
    direction = str(row.get("direction") or "")
    scenario = str(row.get("scenario") or "")
    ts_ms = _i(row.get("ts_ms"), 0)

    feature_cols = model_pack.get("feature_cols") or []
    tf = model_pack.get("feature_transforms") or {}
    sc = model_pack.get("robust_scaler") or {}

    x = build_feature_row(
        feature_cols=feature_cols,
        indicators=indicators,
        direction=direction,
        scenario=scenario,
        ts_ms=ts_ms,
        feature_transforms=tf,
        robust_scaler_params=sc,
    )
    X = np.asarray([x], dtype=np.float32)

    lr = model_pack.get("lr")
    gbdt = model_pack.get("gbdt")
    meta = model_pack.get("meta")

    p_lr = float(lr.predict_proba(X)[0, 1])
    p_g = float(gbdt.predict_proba(X)[0, 1])
    Z = np.asarray([[p_lr, p_g]], dtype=np.float32)
    p = float(meta.predict_proba(Z)[0, 1])

    # Count missing indicator features for data quality tracking
    missing = 0
    try:
        for c in feature_cols:
            if str(c).startswith("f_"):
                k = str(c)[2:]
                if k not in indicators:
                    missing += 1
    except Exception:
        missing = 0

    # Clamp probability to [0, 1], handle NaN
    if not (p == p):
        p = 0.0
    if p < 0.0:
        p = 0.0
    if p > 1.0:
        p = 1.0
    return p, missing


def _metrics_blob(rows: List[Dict[str, Any]], p_list: List[float]) -> Dict[str, float]:
    """Compute shadow metrics dict from rows and predictions."""
    y = np.asarray([_i(r.get("y"), 0) for r in rows], dtype=np.int32)
    rmult = np.asarray([_f(r.get("r_mult"), 0.0) for r in rows], dtype=np.float32)
    p = np.asarray([_f(x, 0.0) for x in p_list], dtype=np.float32)
    return calculate_shadow_metrics(y_true=y, y_prob=p, y_r=rmult)


def _read_env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _read_env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default)) or default))
    except Exception:
        return int(default)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window_hours", type=int, default=int(os.getenv("EDGE_STACK_SHADOW_WINDOW_HOURS", "24")))
    ap.add_argument("--auto_promote_guarded", type=int, default=0)
    args = ap.parse_args(list(argv) if argv is not None else None)

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

    champion_key = os.getenv("EDGE_STACK_CHAMPION_KEY", "cfg:ml_confirm:edge_stack_v1:champion")
    candidate_key = os.getenv("EDGE_STACK_CANDIDATE_KEY", "cfg:ml_confirm:edge_stack_v1:candidate")

    out_dir = os.getenv("EDGE_STACK_SHADOW_OUT_DIR", "/var/lib/trade/of_reports/out/edge_stack")
    os.makedirs(out_dir, exist_ok=True)

    ds_path = os.path.join(out_dir, "shadow_dataset.jsonl")
    ds_report = os.path.join(out_dir, "shadow_dataset_report.json")
    status_path = os.path.join(out_dir, "shadow_status.json")

    metrics_key = os.getenv("EDGE_STACK_SHADOW_METRICS_KEY", "metrics:edge_stack_shadow:last")

    # Guard thresholds (P60)
    max_brier_rel = _read_env_float("EDGE_STACK_PROMOTE_MAX_BRIER_REL", 1.02)
    # Support both old ECE_ABS and new ECE_ABS_DIFF env names for backward compat
    max_ece_abs_diff = _read_env_float(
        "EDGE_STACK_PROMOTE_MAX_ECE_ABS_DIFF",
        _read_env_float("EDGE_STACK_PROMOTE_MAX_ECE_ABS", 0.005),
    )
    min_prec_delta = _read_env_float("EDGE_STACK_PROMOTE_MIN_PREC_DELTA", 0.0)

    # Dataset build window
    end_ms = now_ms()
    start_ms = end_ms - int(args.window_hours) * 3600 * 1000

    # Builder strict mode should match training policy
    strict_cols = _read_env_int("EDGE_STACK_STRICT_FEATURE_COLS", _read_env_int("ML_STRICT_FEATURE_COLS", 0))

    # Build dataset (writes ds_path and ds_report)
    # Use the ml_analysis builder if available; fall back to tools builder
    try:
        from ml_analysis.tools.build_edge_stack_dataset_from_redis import main as build_dataset_main  # type: ignore
    except ImportError:
        from tools.build_edge_stack_dataset_from_redis import main as build_dataset_main  # type: ignore

    try:
        rc = build_dataset_main([
            "--redis_url", redis_url,
            "--since_ms", str(start_ms),
            "--until_ms", str(end_ms),
            "--out_jsonl", ds_path,
            "--out_report_json", ds_report,
            "--strict_feature_cols", str(int(strict_cols)),
            "--diagnose_mismatch", "1",
            "--max_examples", "50",
        ])
    except SystemExit as se:
        rc = int(getattr(se, "code", 1) or 1)

    if rc != 0:
        write_train_metrics(redis_url, metrics_key, {
            "status": "fail_build",
            "success": 0,
            "error": "dataset_build_failed",
        })
        atomic_write_json(status_path, {
            "ts_ms": now_ms(),
            "n": 0,
            "promote_recommended": 0,
            "promote_applied": 0,
            "error": "dataset_build_failed",
        })
        return 2

    try:
        report_obj = json.loads(open(ds_report, "r", encoding="utf-8").read())
    except Exception:
        report_obj = {}

    rows = _read_jsonl(ds_path)
    if not rows:
        write_train_metrics(redis_url, metrics_key, {
            "status": "fail_validate",
            "success": 0,
            "error": "dataset_empty",
            "joined": int(report_obj.get("joined", 0) or 0),
        })
        atomic_write_json(status_path, {
            "ts_ms": now_ms(),
            "n": 0,
            "promote_recommended": 0,
            "promote_applied": 0,
            "error": "dataset_empty",
        })
        return 2

    r = redis.Redis.from_url(redis_url, decode_responses=True)

    champ_cfg = _load_cfg(r, champion_key)
    cand_cfg = _load_cfg(r, candidate_key)

    # Champion is required for comparison; candidate may be missing.
    if not champ_cfg:
        write_train_metrics(redis_url, metrics_key, {
            "status": "fail_cfg",
            "success": 0,
            "error": "missing_champion_cfg",
        })
        atomic_write_json(status_path, {
            "ts_ms": now_ms(),
            "n": int(len(rows)),
            "promote_recommended": 0,
            "promote_applied": 0,
            "error": "missing_champion_cfg",
        })
        return 2

    # Validate schema_version/kind/etc if validator is available
    champ_model_path: str = ""
    cand_model_path: str = ""
    if validate_champion_cfg is not None:
        try:
            champ_cfg_valid, _info = validate_champion_cfg(json.dumps(champ_cfg, ensure_ascii=False))
            champ_model_path = champ_cfg_valid.model_path
        except Exception:
            champ_model_path = str(champ_cfg.get("model_path", ""))
    else:
        champ_model_path = str(champ_cfg.get("model_path", ""))

    champ_pack = joblib.load(champ_model_path)

    # Evaluate champion on dataset
    champ_p: List[float] = []
    champ_missing_sum = 0
    for rr in rows:
        p, miss = _predict_pack_p(champ_pack, rr)
        champ_p.append(p)
        champ_missing_sum += int(miss)

    champ_metrics_raw = _metrics_blob(rows, champ_p)
    # calibrated metrics placeholder — apply calibrator here if/when it exists
    champ_metrics_cal = dict(champ_metrics_raw)

    # Candidate evaluation (optional)
    cand_metrics_raw: Dict[str, float] = {}
    cand_metrics_cal: Dict[str, float] = {}
    cand_missing_sum = 0
    cand_pack = None
    cand_cfg_valid = None

    if cand_cfg:
        try:
            if validate_champion_cfg is not None:
                cand_cfg_valid, _info2 = validate_champion_cfg(json.dumps(cand_cfg, ensure_ascii=False))
                cand_model_path = cand_cfg_valid.model_path
            else:
                cand_model_path = str(cand_cfg.get("model_path", ""))
            cand_pack = joblib.load(cand_model_path)
        except Exception:
            cand_pack = None

    cand_p: List[float] = []
    if cand_pack is not None:
        for rr in rows:
            p, miss = _predict_pack_p(cand_pack, rr)
            cand_p.append(p)
            cand_missing_sum += int(miss)
        cand_metrics_raw = _metrics_blob(rows, cand_p)
        cand_metrics_cal = dict(cand_metrics_raw)

    # Promotion decision
    promote_recommended = 0
    promote_reasons: List[str] = []
    if cand_pack is not None:
        ok, reasons = check_promotion_guard(
            champion_metrics=champ_metrics_cal,
            candidate_metrics=cand_metrics_cal,
            max_brier_rel=float(max_brier_rel),
            max_ece_abs_diff=float(max_ece_abs_diff),
            min_prec_delta=float(min_prec_delta),
        )
        promote_recommended = 1 if ok else 0
        promote_reasons = reasons

    promote_applied = 0

    # Guarded autopromotion: copy candidate model to stable champion path + update champion cfg
    if int(args.auto_promote_guarded) == 1 and promote_recommended == 1 and cand_model_path:
        try:
            edge_dir = os.getenv("EDGE_STACK_V1_DIR", "/var/lib/trade/ml_models/edge_stack_v1")
            os.makedirs(os.path.join(edge_dir, "champions"), exist_ok=True)
            dst_model = os.path.join(edge_dir, "champions", "edge_stack_v1_champion.joblib")
            atomic_copy(cand_model_path, dst_model)

            new_cfg = dict(cand_cfg)
            new_cfg["schema_version"] = 1
            new_cfg["kind"] = "edge_stack_v1"
            new_cfg["model_path"] = dst_model
            new_cfg["created_ms"] = now_ms()
            # keep candidate run_id for traceability
            new_cfg.setdefault("promoted_from", str(candidate_key))

            r.set(champion_key, json.dumps(new_cfg, ensure_ascii=False, separators=(",", ":")))
            promote_applied = 1
        except Exception as e:
            promote_recommended = 0
            promote_reasons = [f"promote_failed:{type(e).__name__}"]
            promote_applied = 0

    # Write status JSON for file-based exporter (edge_stack_shadow_status_exporter_v1)
    status_obj = {
        "ts_ms": now_ms(),
        "n": int(len(rows)),
        "joined": int(report_obj.get("joined", len(rows)) or len(rows)),
        "pos_rate": float(report_obj.get("pos_rate", 0.0) or 0.0),
        "promote_recommended": int(promote_recommended),
        "promote_applied": int(promote_applied),
        "promote_reasons": list(promote_reasons)[:10],
        "champion": {
            "cfg": {
                "run_id": str(champ_cfg.get("run_id", "")),
                "model_path": str(champ_model_path),
            },
            "metrics": {"raw": champ_metrics_raw, "cal": champ_metrics_cal},
            "missing_sum": int(champ_missing_sum),
        },
        "candidate": {
            "cfg": {
                "run_id": str(cand_cfg.get("run_id", "")) if isinstance(cand_cfg, dict) else "",
                "model_path": str(cand_model_path or ""),
            },
            "metrics": {"raw": cand_metrics_raw, "cal": cand_metrics_cal} if cand_pack is not None else {},
            "missing_sum": int(cand_missing_sum),
        },
    }
    atomic_write_json(status_path, status_obj)

    # Write Redis metrics hash for Prometheus alerts (P60)
    out_metrics: Dict[str, Any] = {
        "status": "ok",
        "success": 1,
        "window_hours": int(args.window_hours),
        "joined": int(report_obj.get("joined", len(rows)) or len(rows)),
        "pos_rate": float(report_obj.get("pos_rate", 0.0) or 0.0),
        "promote_recommended": int(promote_recommended),
        "promote_applied": int(promote_applied),
        "promote_reasons": promote_reasons,
        "champion_brier": float(champ_metrics_cal.get("brier", 0.0) or 0.0),
        "champion_ece": float(champ_metrics_cal.get("ece", 0.0) or 0.0),
        "champion_precision_top5pct": float(champ_metrics_cal.get("precision_top5pct", 0.0) or 0.0),
        "champion_expectancy_r_top5pct": float(champ_metrics_cal.get("expectancy_r_top5pct", 0.0) or 0.0),
        "candidate_brier": float(cand_metrics_cal.get("brier", 0.0) or 0.0),
        "candidate_ece": float(cand_metrics_cal.get("ece", 0.0) or 0.0),
        "candidate_precision_top5pct": float(cand_metrics_cal.get("precision_top5pct", 0.0) or 0.0),
        "candidate_expectancy_r_top5pct": float(cand_metrics_cal.get("expectancy_r_top5pct", 0.0) or 0.0),
    }
    write_train_metrics(redis_url, metrics_key, out_metrics)

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
