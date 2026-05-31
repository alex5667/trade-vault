from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import redis

from common.model_registry import ensure_dir, promote_version, write_versioned_model
from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


def now_ms() -> int:
    return get_ny_time_millis()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _redis(url: str) -> redis.Redis:
    return redis.Redis.from_url(url, decode_responses=True)


def _run(cmd: list[str]) -> None:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"cmd_failed rc={p.returncode} cmd={' '.join(cmd)}\n{p.stdout}")


def _write_redis_status(r: redis.Redis, key: str, obj: dict[str, Any], ttl_sec: int = 7 * 86400) -> None:
    r.set(key, json.dumps(obj, ensure_ascii=False, sort_keys=True))
    if ttl_sec > 0:
        r.expire(key, ttl_sec)


def _select_paths(args: argparse.Namespace) -> tuple[str, str, str]:
    out_dir = ensure_dir(args.out_dir)
    ds_path = str(Path(out_dir) / "meta_model_ds.jsonl")
    model_path = str(Path(out_dir) / "meta_lr.json")
    report_path = str(Path(out_dir) / "meta_lr.report.json")
    return ds_path, model_path, report_path


def _find_script(path: str) -> str:
    if os.path.exists(path):
        return path
    # try without python-worker/ prefix if we are already inside it (Docker)
    if path.startswith("python-worker/"):
        alt = path[len("python-worker/") :]
        if os.path.exists(alt):
            return alt
    return path


def build_dataset_from_redis(args: argparse.Namespace, ds_path: str) -> None:
    """
    Calls existing builder to create jsonl dataset. We call via subprocess for import-path stability.
    """
    builder = _find_script(args.redis_builder)
    cmd = [
        sys.executable,
        builder,
        "--redis_url",
        args.redis_url,
        "--signal_stream",
        args.signal_stream,
        "--closed_stream",
        args.closed_stream,
        "--out_jsonl",
        ds_path,
        "--out_report_json",
        str(Path(ds_path).with_suffix(".build_report.json")),
        "--label_source",
        args.label_source,
    ]
    if args.tb_labels_stream:
        cmd += ["--tb_labels_stream", args.tb_labels_stream, "--tb_labels_field", args.tb_labels_field, "--tb_labels_count", str(args.tb_labels_count)]
        if args.label_source == "tb_util":
            cmd += ["--tb_util_min_r", str(args.tb_util_min_r)]
    if args.symbol:
        cmd += ["--symbol", args.symbol]
    if args.since_ms:
        cmd += ["--since_ms", str(args.since_ms)]
    _run(cmd)


def train_meta_lr(args: argparse.Namespace, ds_path: str, model_path: str, report_path: str) -> None:
    """
    Calls trainer with purged/embargo split. Uses subprocess for stability.
    """
    trainer = _find_script(args.trainer)
    cmd = [
        sys.executable,
        trainer,
        "--in_jsonl",
        ds_path,
        "--out_json",
        model_path,
        "--out_report_json",
        report_path,
        "--label_col",
        "y",
        "--time_col",
        "ts_ms",
        "--purge_ms",
        str(args.purge_ms),
        "--embargo_ms",
        str(args.embargo_ms),
    ]
    if args.feature_prefix:
        cmd += ["--feature_prefix", args.feature_prefix]
    _run(cmd)


def _load_report(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _passes_gates(report: dict[str, Any], args: argparse.Namespace) -> tuple[bool, str]:
    n = int(report.get("n", 0) or 0)
    auc = float(report.get("auc", 0.0) or 0.0)
    brier = float(report.get("brier", 1.0) or 1.0)
    ece = float(report.get("ece10", 1.0) or 1.0)
    pos = float(report.get("pos_rate", 0.0) or 0.0)
    if n < args.min_n:
        return False, f"dataset_too_small n={n} need>={args.min_n}"
    if auc < args.min_auc:
        return False, f"auc_low auc={auc:.3f} min={args.min_auc:.3f}"
    if brier > args.max_brier:
        return False, f"brier_high brier={brier:.4f} max={args.max_brier:.4f}"
    if ece > args.max_ece:
        return False, f"ece_high ece={ece:.4f} max={args.max_ece:.4f}"
    if not (args.pos_rate_min <= pos <= args.pos_rate_max):
        return False, f"pos_rate_out_of_range pos={pos:.3f} range=[{args.pos_rate_min:.3f},{args.pos_rate_max:.3f}]"
    return True, "ok"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--signal_stream", default=os.getenv("ML_REPLAY_STREAM", RS.ML_REPLAY_INPUTS))
    ap.add_argument("--closed_stream", default=os.getenv("TRADES_CLOSED_STREAM", RS.TRADES_CLOSED))
    ap.add_argument("--tb_labels_stream", default=os.getenv("TB_LABELS_STREAM", RS.TB_LABELS))
    ap.add_argument("--tb_labels_field", default=os.getenv("TB_LABELS_FIELD", "payload"))
    ap.add_argument("--tb_labels_count", type=int, default=_env_int("TB_LABELS_COUNT", 200000))
    ap.add_argument("--label_source", choices=["closed", "tb_primary", "tb_util"], default=os.getenv("LABEL_SOURCE", "closed"))
    ap.add_argument("--tb_util_min_r", type=float, default=_env_float("TB_UTIL_MIN_R", 0.0))
    ap.add_argument("--symbol", default=os.getenv("SYMBOL", ""))
    ap.add_argument("--since_ms", type=int, default=_env_int("SINCE_MS", 0))

    ap.add_argument("--redis_builder", default="python-worker/ml_analysis/tools/build_edge_stack_dataset_from_redis.py")
    ap.add_argument("--trainer", default="python-worker/tools/train_meta_model_lr_purged_v2.py")

    ap.add_argument("--out_dir", default=os.getenv("META_TRAIN_OUT_DIR", "/var/lib/trade/of_reports/models/meta_train"))
    ap.add_argument("--registry_dir", default=os.getenv("META_MODEL_REGISTRY_DIR", "/var/lib/trade/of_reports/models/meta_registry"))
    ap.add_argument("--meta_model_path", default=os.getenv("META_MODEL_PATH", "/var/lib/trade/of_reports/models/meta_lr.json"))

    ap.add_argument("--purge_ms", type=int, default=_env_int("META_PURGE_MS", 300000))
    ap.add_argument("--embargo_ms", type=int, default=_env_int("META_EMBARGO_MS", 60000))
    ap.add_argument("--feature_prefix", default=os.getenv("FEATURE_PREFIX", ""))

    ap.add_argument("--min_n", type=int, default=_env_int("META_MIN_N", 400))
    ap.add_argument("--min_auc", type=float, default=_env_float("META_MIN_AUC", 0.52))
    ap.add_argument("--max_brier", type=float, default=_env_float("META_MAX_BRIER", 0.265))
    ap.add_argument("--max_ece", type=float, default=_env_float("META_MAX_ECE", 0.08))
    ap.add_argument("--pos_rate_min", type=float, default=_env_float("META_POS_RATE_MIN", 0.15))
    ap.add_argument("--pos_rate_max", type=float, default=_env_float("META_POS_RATE_MAX", 0.85))

    ap.add_argument("--apply", action="store_true", help="If set, promote to META_MODEL_PATH when gates pass.")
    ap.add_argument("--dry_run", action="store_true")
    args = ap.parse_args()

    r = _redis(args.redis_url)
    t0 = now_ms()
    ds_path, model_path, report_path = _select_paths(args)

    status: dict[str, Any] = {
        "ts_ms": t0,
        "label_source": args.label_source,
        "symbol": args.symbol or "ALL",
        "streams": {"signals": args.signal_stream, "closed": args.closed_stream, "tb": args.tb_labels_stream},
        "out_dir": args.out_dir,
        "registry_dir": args.registry_dir,
        "meta_model_path": args.meta_model_path,
    }

    try:
        build_dataset_from_redis(args, ds_path)
        status["dataset_jsonl"] = ds_path

        # Check dataset size and diversity
        line_count = 0
        labels = set()
        if os.path.exists(ds_path):
            with open(ds_path, encoding="utf-8") as f:
                for line in f:
                    try:
                        line_count += 1
                        row = json.loads(line)
                        if "y" in row:
                            labels.add(row["y"])
                    except Exception:
                        continue

        if line_count < args.min_n:
            status["applied"] = False
            status["reason"] = f"skip:dataset_too_small n={line_count} need>={args.min_n}"
            status["duration_ms"] = now_ms() - t0
            _write_redis_status(r, "meta_model:last_train_report", status)
            r.set("meta_model:last_train_ts_ms", t0)
            r.set("meta_model:last_status", status["reason"])
            print(f"Dataset too small ({line_count} rows), skipping training.")
            return

        if len(labels) < 2:
            status["applied"] = False
            status["reason"] = f"skip:dataset_single_class labels={list(labels)} n={line_count}"
            status["duration_ms"] = now_ms() - t0
            _write_redis_status(r, "meta_model:last_train_report", status)
            r.set("meta_model:last_train_ts_ms", t0)
            r.set("meta_model:last_status", status["reason"])
            print(f"Dataset has only one class ({list(labels)}), skipping training to avoid fit error.")
            return

        train_meta_lr(args, ds_path, model_path, report_path)
        rep = _load_report(report_path)
        status["report"] = rep
        ok, reason = _passes_gates(rep, args)
        status["gate_ok"] = ok
        status["reason"] = reason

        # store versioned model regardless (useful for debugging)
        wr, version = write_versioned_model(model_path, args.registry_dir, kind="meta_lr", extra_meta={"report": rep, "label_source": args.label_source})
        status["registry_version"] = version
        status["registry_sha256"] = wr.sha256

        if args.apply and ok and not args.dry_run:
            pointer = promote_version(args.registry_dir, "meta_lr", version, args.meta_model_path)
            status["applied"] = True
            status["champion"] = pointer
            r.set("meta_model:last_apply_ts_ms", now_ms())
        else:
            status["applied"] = False

        status["duration_ms"] = now_ms() - t0
        _write_redis_status(r, "meta_model:last_train_report", status)
        r.set("meta_model:last_train_ts_ms", t0)
        if ok:
            r.set("meta_model:last_status", "ok")
        else:
            r.set("meta_model:last_status", f"fail:{reason}")
    except Exception as e:
        status["error"] = str(e)
        status["duration_ms"] = now_ms() - t0
        _write_redis_status(r, "meta_model:last_train_report", status)
        r.set("meta_model:last_train_ts_ms", t0)
        r.set("meta_model:last_status", f"err:{type(e).__name__}")
        raise


if __name__ == "__main__":
    main()
