#!/usr/bin/env python3
from __future__ import annotations

"""Nightly orchestrator for OFC contextual Patch B/C workflow.

Runs:
1) build_ofc_contextual_dataset_v1.py
2) train_ofc_exec_cost_v1.py
3) train_ofc_rule_success_v1.py
4) build_ofc_contextual_bundle_v1.py

Writes summary metrics hash into Redis for exporter/alerts.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("nightly_ofc_contextual_ops_bundle")

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


def _now_ms() -> int:
    return int(time.time() * 1000)


def _script(rel_path: str) -> str:
    base = Path(__file__).resolve().parents[1]
    path = base / rel_path
    return str(path)


def _run(cmd: List[str]) -> int:
    logger.info("RUNNING: %s", " ".join(cmd))
    try:
        return subprocess.run(cmd, check=False).returncode
    except Exception as e:
        logger.error("Command failed: %s", e)
        return 1


def _write_metrics_hash(mapping: Dict[str, Any]) -> None:
    if redis is None:
        return
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    metrics_key = os.getenv("OFC_CTX_OPS_METRICS_KEY", "metrics:ofc_contextual_ops_bundle")
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=False)
        r.hset(metrics_key, mapping={str(k): str(v) for k, v in mapping.items()})
    except Exception:
        return


def _read_manifest_created_ts(bundle_dir: str) -> int:
    try:
        p = Path(bundle_dir) / "manifest.json"
        obj = json.loads(p.read_text(encoding="utf-8"))
        return int(obj.get("created_ts_ms") or obj.get("ts_ms") or 0)
    except Exception:
        return 0


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Nightly OFC contextual ops bundle")
    ap.add_argument("--decisions_jsonl", default=os.getenv("OFC_CTX_DECISIONS_JSONL", "./data/ofc_ctx/decisions.jsonl"))
    ap.add_argument("--outcomes_jsonl", default=os.getenv("OFC_CTX_OUTCOMES_JSONL", "./data/ofc_ctx/outcomes.jsonl"))
    ap.add_argument("--work_dir", default=os.getenv("OFC_CTX_WORK_DIR", "./artifacts/ofc_ctx"))
    ap.add_argument("--registry_dir", default=os.getenv("OFC_CTX_REGISTRY_DIR", "./artifacts/ofc_ctx/registry"))
    ap.add_argument("--bundle_out_dir", default=os.getenv("OFC_CTX_BUNDLE_OUT_DIR", "./artifacts/ofc_ctx/current"))
    ap.add_argument("--success_bps", type=float, default=float(os.getenv("OFC_CTX_SUCCESS_BPS", "0.0") or 0.0))
    ap.add_argument("--emit-metrics", action="store_true")
    args = ap.parse_args(argv)

    start_ts = _now_ms()
    work_dir = Path(args.work_dir)
    ds_dir = work_dir / "datasets"
    model_dir = work_dir / "models"
    ds_dir.mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)

    dataset_report = ds_dir / "dataset_report.json"
    exec_ds = ds_dir / "exec_cost_train.jsonl"
    rule_ds = ds_dir / "rule_success_train.jsonl"
    exec_model = model_dir / "exec_cost_model.json"
    rule_model = model_dir / "rule_success_model.json"
    bundle_dir = model_dir / "bundle"
    gate_cfg = model_dir / "gate_cfg.json"
    gate_cfg.write_text(json.dumps({
        "p_min_default": float(os.getenv("OFC_CTX_P_MIN_DEFAULT", "0.55") or 0.55),
        "edge_floor_p50_bps": float(os.getenv("OFC_CTX_EDGE_FLOOR_P50_BPS", "0.0") or 0.0),
        "edge_floor_p90_bps": float(os.getenv("OFC_CTX_EDGE_FLOOR_P90_BPS", "-2.0") or -2.0),
    }, indent=2, sort_keys=True), encoding="utf-8")

    py = sys.executable
    rc_dataset = _run([
        py,
        _script("ml_analysis/tools/build_ofc_contextual_dataset_v1.py"),
        "--decisions_jsonl", str(args.decisions_jsonl),
        "--outcomes_jsonl", str(args.outcomes_jsonl),
        "--out_exec_cost_jsonl", str(exec_ds),
        "--out_rule_success_jsonl", str(rule_ds),
        "--out_report_json", str(dataset_report),
        "--success_bps", str(args.success_bps),
    ])
    if rc_dataset != 0:
        if args.emit_metrics:
            _write_metrics_hash({"last_run_ts_ms": start_ts, "last_ok": 0, "last_exit_code": rc_dataset})
        return rc_dataset

    rc_exec = _run([py, _script("ml_analysis/tools/train_ofc_exec_cost_v1.py"), "--train_jsonl", str(exec_ds), "--out_model_json", str(exec_model)])
    if rc_exec != 0:
        if args.emit_metrics:
            _write_metrics_hash({"last_run_ts_ms": start_ts, "last_ok": 0, "last_exit_code": rc_exec})
        return rc_exec

    rc_rule = _run([py, _script("ml_analysis/tools/train_ofc_rule_success_v1.py"), "--train_jsonl", str(rule_ds), "--out_model_json", str(rule_model)])
    if rc_rule != 0:
        if args.emit_metrics:
            _write_metrics_hash({"last_run_ts_ms": start_ts, "last_ok": 0, "last_exit_code": rc_rule})
        return rc_rule

    rc_bundle = _run([
        py,
        _script("ml_analysis/tools/build_ofc_contextual_bundle_v1.py"),
        "--exec_cost_model_path", str(exec_model),
        "--rule_success_model_path", str(rule_model),
        "--gate_cfg_json", str(gate_cfg),
        "--out_bundle_dir", str(bundle_dir),
        "--registry_dir", str(args.registry_dir),
        "--promote_dst_dir", str(args.bundle_out_dir),
    ])
    created_ts_ms = _read_manifest_created_ts(str(args.bundle_out_dir)) if rc_bundle == 0 else 0
    if args.emit_metrics:
        _write_metrics_hash(
            {
                "last_run_ts_ms": _now_ms(),
                "last_ok": 1 if rc_bundle == 0 else 0,
                "last_exit_code": rc_bundle,
                "bundle_created_ts_ms": created_ts_ms,
                "bundle_out_dir": str(args.bundle_out_dir),
            }
        )
    return rc_bundle


if __name__ == "__main__":
    raise SystemExit(main())
