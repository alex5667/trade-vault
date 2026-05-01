from __future__ import annotations
"""Nightly bundle: minimal feature selection loop (importance + stability by regime/session).

Goal
  Produce a daily report to quickly prune/flag "noise" features before they spread into
  training/deploy loops. Designed for:
    - low latency / deterministic pipeline
    - low-cardinality Prometheus alerting

Pipeline
  1) Build edge-stack dataset window (signals:of:inputs + trades:closed)
  2) Run ml_analysis.tools.feature_selection_loop_v1
  3) Publish summary hash to Redis (metrics:feature_selection_loop:last)

Artifacts
  <out_dir>/runs/<run_id>/edge_train.jsonl
  <out_dir>/runs/<run_id>/edge_dataset_report.json
  <out_dir>/runs/<run_id>/feature_selection/summary.json + report.md + CSVs

Redis metrics (low-cardinality)
  status, success, run_id, updated_ts_ms, schema_ver, schema_hash,
  n_rows, n_features, auc_val, brier_val, noise_n,
  out_dir, run_dir, summary_path, report_path
"""


import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from ml_analysis.tools.edge_stack_train_bundle_utils_p59 import atomic_write_json, now_ms, write_train_metrics

try:
    from tools.schema_choices_v1 import schema_choices as _schema_choices, normalize_schema_ver as _norm_schema_ver  # type: ignore
except Exception:
    from ml_analysis.tools.schema_choices_v1 import schema_choices as _schema_choices, normalize_schema_ver as _norm_schema_ver  # type: ignore



logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("nightly_feature_selection_loop_bundle_v1")


def _run(module: str, args: list, timeout: int = 3600) -> Tuple[bool, int]:
    cmd = [sys.executable, "-m", module] + list(args)
    logger.info("Running: %s", " ".join(cmd))
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0:
        logger.error("Command failed code=%s\nSTDOUT:%s\nSTDERR:%s", p.returncode, p.stdout, p.stderr)
    return p.returncode == 0, int(p.returncode)


def _load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _write_metrics(redis_url: str, metrics_key: str, mapping: Dict[str, Any]) -> None:
    try:
        write_train_metrics(str(redis_url), str(metrics_key), mapping)
    except Exception:
        return


def _fail(
    *,
    redis_url: str,
    metrics_key: str,
    run_id: str,
    out_dir: str,
    run_dir: str,
    status: str,
    reason: str,
    exit_code: int,
    latest_json: str,
    version_json: str,
) -> int:
    mapping: Dict[str, Any] = {
        "status": status,
        "reason": reason,
        "success": 0,
        "run_id": run_id,
        "exit_code": int(exit_code),
        "updated_ts_ms": now_ms(),
        "out_dir": str(out_dir),
        "run_dir": str(run_dir),
    }
    _write_metrics(redis_url, metrics_key, mapping)
    atomic_write_json(version_json, mapping)
    atomic_write_json(latest_json, mapping)
    return 2


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(description="Nightly minimal feature selection loop bundle")
    ap.add_argument("--redis_url", default=os.environ.get("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--metrics_key", default=os.environ.get("FEATURE_SELECTION_LOOP_METRICS_KEY", "metrics:feature_selection_loop:last"))

    ap.add_argument("--out_dir", default=os.environ.get("FEATURE_SELECTION_LOOP_OUT_DIR", "/var/lib/trade/ml_models/feature_selection_loop_v1"))
    ap.add_argument("--signals_stream", default=os.environ.get("EDGE_STACK_SIGNALS_STREAM", "signals:of:inputs"))
    ap.add_argument("--closed_stream", default=os.environ.get("EDGE_STACK_CLOSED_STREAM", "trades:closed"))
    ap.add_argument("--window_hours", type=int, default=int(os.environ.get("FEATURE_SELECTION_LOOP_WINDOW_HOURS", "168")))
    ap.add_argument("--signals_count", type=int, default=int(os.environ.get("FEATURE_SELECTION_LOOP_SIGNALS_COUNT", "250000")))
    ap.add_argument("--closes_count", type=int, default=int(os.environ.get("FEATURE_SELECTION_LOOP_CLOSES_COUNT", "250000")))
    ap.add_argument("--y_min_r", type=float, default=float(os.environ.get("Y_MIN_R", "0.10")))

    ap.add_argument(
        "--feature_schema_ver",
        default=os.environ.get("FEATURE_SELECTION_LOOP_SCHEMA_VER", os.environ.get("ML_FEATURE_SCHEMA_VER", "v5_of")),
        choices=_schema_choices(include_empty=True),
    )
    ap.add_argument("--scenario_prefix", default=os.environ.get("EDGE_STACK_SCENARIO_PREFIX", "bucket:"))
    ap.add_argument("--include_time_onehot", type=int, default=int(os.environ.get("EDGE_STACK_INCLUDE_TIME_ONEHOT", "1")))
    ap.add_argument("--strict_feature_cols", type=int, default=int(os.environ.get("ML_STRICT_FEATURE_COLS", "0")))
    ap.add_argument("--forbid_scenario_v4_onehot", type=int, default=int(os.environ.get("ML_FORBID_SCENARIO_V4_ONEHOT", "0")))

    ap.add_argument("--model", default=os.environ.get("FEATURE_SELECTION_LOOP_MODEL", "lr"), choices=["lr", "gbdt"])
    ap.add_argument("--min_rows", type=int, default=int(os.environ.get("FEATURE_SELECTION_LOOP_MIN_ROWS", "4000")))
    ap.add_argument("--min_group_rows", type=int, default=int(os.environ.get("FEATURE_SELECTION_LOOP_MIN_GROUP_ROWS", "300")))
    ap.add_argument("--n_repeats", type=int, default=int(os.environ.get("FEATURE_SELECTION_LOOP_N_REPEATS", "3")))
    ap.add_argument("--max_features", type=int, default=int(os.environ.get("FEATURE_SELECTION_LOOP_MAX_FEATURES", "0")))
    ap.add_argument("--seed", type=int, default=int(os.environ.get("FEATURE_SELECTION_LOOP_SEED", "7")))

    args = ap.parse_args(argv)

    feature_schema_ver = _norm_schema_ver(str(args.feature_schema_ver or "").strip())


    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.abspath(str(args.out_dir))
    run_dir = os.path.join(out_dir, "runs", run_id)
    os.makedirs(run_dir, exist_ok=True)
    os.makedirs(os.path.join(out_dir, "versions"), exist_ok=True)

    latest_json = os.path.join(out_dir, "feature_selection_loop_latest.json")
    version_json = os.path.join(out_dir, "versions", f"feature_selection_loop_{run_id}.json")

    dataset_jsonl = os.path.join(run_dir, "edge_train.jsonl")
    dataset_report = os.path.join(run_dir, "edge_dataset_report.json")
    quarantine_jsonl = os.path.join(run_dir, "edge_quarantine.jsonl")
    feature_cols_json = os.path.join(run_dir, "feature_cols.json")
    fs_dir = os.path.join(run_dir, "feature_selection")
    os.makedirs(fs_dir, exist_ok=True)

    end_ms = now_ms()
    start_ms = end_ms - int(args.window_hours) * 3600 * 1000

    build_args = [
        "--redis_url",
        str(args.redis_url),
        "--signal_stream",
        str(args.signals_stream),
        "--closed_stream",
        str(args.closed_stream),
        "--signals_count",
        str(args.signals_count),
        "--closes_count",
        str(args.closes_count),
        "--since_ms",
        str(start_ms),
        "--until_ms",
        str(end_ms),
        "--y_min_r",
        str(args.y_min_r),
        "--out_jsonl",
        dataset_jsonl,
        "--out_report_json",
        dataset_report,
        "--out_quarantine_jsonl",
        quarantine_jsonl,
        "--emit_feature_cols_json",
        feature_cols_json,
        "--feature_schema_ver",
        str(feature_schema_ver or "").strip(),
        "--scenario_prefix",
        str(args.scenario_prefix),
        "--include_time_onehot",
        str(int(args.include_time_onehot)),
        "--strict_feature_cols",
        str(int(args.strict_feature_cols)),
        "--forbid_scenario_v4_onehot",
        str(int(args.forbid_scenario_v4_onehot)),
    ]
    ok_build, rc_build = _run("ml_analysis.tools.build_edge_stack_dataset_from_redis", build_args, timeout=3600)
    if (not ok_build) or (not os.path.exists(dataset_report)):
        return _fail(
            redis_url=str(args.redis_url),
            metrics_key=str(args.metrics_key),
            run_id=run_id,
            out_dir=out_dir,
            run_dir=run_dir,
            status="fail_build",
            reason="dataset_build_failed",
            exit_code=rc_build,
            latest_json=latest_json,
            version_json=version_json,
        )

    rep = _load_json(dataset_report)
    joined = int(rep.get("joined", 0) or 0)
    if joined < int(args.min_rows):
        return _fail(
            redis_url=str(args.redis_url),
            metrics_key=str(args.metrics_key),
            run_id=run_id,
            out_dir=out_dir,
            run_dir=run_dir,
            status="fail_validate",
            reason=f"too_few_rows joined={joined} min_rows={int(args.min_rows)}",
            exit_code=3,
            latest_json=latest_json,
            version_json=version_json,
        )

    loop_args = [
        "--data_path",
        dataset_jsonl,
        "--schema_ver",
        str(feature_schema_ver),
        "--out_dir",
        fs_dir,
        "--model",
        str(args.model),
        "--min_group_rows",
        str(int(args.min_group_rows)),
        "--n_repeats",
        str(int(args.n_repeats)),
        "--seed",
        str(int(args.seed)),
    ]
    if int(args.max_features) > 0:
        loop_args += ["--max_features", str(int(args.max_features))]

    ok_loop, rc_loop = _run("ml_analysis.tools.feature_selection_loop_v1", loop_args, timeout=3600)
    summary_path = os.path.join(fs_dir, "summary.json")
    if (not ok_loop) or (not os.path.exists(summary_path)):
        return _fail(
            redis_url=str(args.redis_url),
            metrics_key=str(args.metrics_key),
            run_id=run_id,
            out_dir=out_dir,
            run_dir=run_dir,
            status="fail_loop",
            reason="feature_selection_loop_failed",
            exit_code=rc_loop,
            latest_json=latest_json,
            version_json=version_json,
        )

    summary = _load_json(summary_path)
    noise_n = int(len(summary.get("noise_examples") or []))

    mapping: Dict[str, Any] = {
        "status": "ok",
        "reason": "",
        "success": 1,
        "run_id": run_id,
        "exit_code": 0,
        "updated_ts_ms": now_ms(),
        "schema_ver": str(summary.get("schema_ver") or str(feature_schema_ver)),
        "schema_hash": str(summary.get("schema_hash") or ""),
        "n_rows": int(summary.get("n_rows", 0) or 0),
        "n_features": int(summary.get("n_features", 0) or 0),
        "auc_val": float(summary.get("auc_val", 0.0) or 0.0),
        "brier_val": float(summary.get("brier_val", 0.0) or 0.0),
        "noise_n": int(noise_n),
        "out_dir": str(out_dir),
        "run_dir": str(run_dir),
        "summary_path": str(summary_path),
        "report_path": str(os.path.join(fs_dir, "report.md")),
    }
    _write_metrics(str(args.redis_url), str(args.metrics_key), mapping)
    atomic_write_json(version_json, mapping)
    atomic_write_json(latest_json, mapping)

    logger.info(
        "OK run_id=%s joined=%s n_features=%s noise_n=%s auc=%.4f brier=%.6f",
        run_id,
        mapping["n_rows"],
        mapping["n_features"],
        mapping["noise_n"],
        float(mapping["auc_val"]),
        float(mapping["brier_val"]),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
