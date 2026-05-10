from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""P59: Nightly edge_stack_v1 training bundle (dataset build + OOF train + publish).

This bundle:
  1) builds an edge_stack dataset from Redis streams with archive fallback (P58),
  2) validates dataset health (min_joined, pos_rate range),
  3) trains edge_stack_v1 via OOF stacking (ml_analysis.tools.train_edge_stack_v1_oof),
  4) publishes artifacts (candidate; optional champion) and writes Redis metrics for SRE.

Environment defaults:
  REDIS_URL
  SIGNAL_STREAM / CLOSED_STREAM
  EDGE_STACK_V1_DIR (default /var/lib/trade/ml_models/edge_stack_v1)
  EDGE_STACK_AUTO_PROMOTE (default 0)
  EDGE_STACK_MIN_JOINED (default 200)
  EDGE_STACK_POS_RATE_MIN (default 0.05)
  EDGE_STACK_POS_RATE_MAX (default 0.60)
"""


import argparse
import json
import os
import subprocess
import sys
import time
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from ml_analysis.tools.edge_stack_train_bundle_utils_p59 import (
    atomic_copy,
    atomic_write_json,
    now_ms,
    validate_dataset_report,
    write_train_metrics,
)

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


def _run(cmd: Sequence[str], timeout_s: int) -> tuple[int, str, str]:
    p = subprocess.run(list(cmd), capture_output=True, text=True, timeout=timeout_s)
    return int(p.returncode), str(p.stdout or ""), str(p.stderr or "")


def _last_json_line(stdout: str) -> dict[str, Any]:
    lines = [ln.strip() for ln in (stdout or "").splitlines() if ln.strip()]
    if not lines:
        return {}
    # train tool prints report JSON as the last line
    try:
        return json.loads(lines[-1])
    except Exception:
        return {}


def _redis_set_json(redis_url: str, key: str, obj: dict[str, Any], ttl_s: int = 0) -> None:
    if redis is None:
        return
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        r.set(key, json.dumps(obj, ensure_ascii=False, sort_keys=True))
        if ttl_s and int(ttl_s) > 0:
            r.expire(key, int(ttl_s))
    except Exception:
        return


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window_hours", type=int, default=int(os.getenv("EDGE_STACK_WINDOW_HOURS", "72")))
    ap.add_argument("--run_id", default="")
    ap.add_argument("--auto_promote", type=int, default=int(os.getenv("EDGE_STACK_AUTO_PROMOTE", "0")))

    ap.add_argument("--min_joined", type=int, default=int(os.getenv("EDGE_STACK_MIN_JOINED", "200")))
    ap.add_argument("--pos_rate_min", type=float, default=float(os.getenv("EDGE_STACK_POS_RATE_MIN", "0.05")))
    ap.add_argument("--pos_rate_max", type=float, default=float(os.getenv("EDGE_STACK_POS_RATE_MAX", "0.60")))

    ap.add_argument("--train_n_splits", type=int, default=int(os.getenv("EDGE_STACK_N_SPLITS", "5")))
    ap.add_argument("--train_purge_ms", type=int, default=int(os.getenv("EDGE_STACK_PURGE_MS", "300000")))
    ap.add_argument("--train_embargo_ms", type=int, default=int(os.getenv("EDGE_STACK_EMBARGO_MS", "120000")))
    ap.add_argument("--train_p_min", type=float, default=float(os.getenv("EDGE_STACK_P_MIN", "0.55")))

    ap.add_argument("--redis_url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--signal_stream", default=os.getenv("SIGNAL_STREAM", RS.OF_INPUTS))
    ap.add_argument("--closed_stream", default=os.getenv("CLOSED_STREAM", RS.TRADES_CLOSED))

    ap.add_argument("--signals_count", type=int, default=int(os.getenv("SIGNALS_COUNT", "200000")))
    ap.add_argument("--closes_count", type=int, default=int(os.getenv("CLOSES_COUNT", "200000")))

    ap.add_argument("--y_min_r", type=float, default=float(os.getenv("Y_MIN_R", "0.10")))

    ap.add_argument("--file_fallback", type=int, default=int(os.getenv("FILE_FALLBACK", "1")))
    ap.add_argument("--archive_lookback_days", type=int, default=int(os.getenv("ARCHIVE_LOOKBACK_DAYS", "7")))
    ap.add_argument("--signal_archive_dir", default=os.getenv("SIGNALS_ARCHIVE_DIR", ""))
    ap.add_argument("--closed_archive_dir", default=os.getenv("TRADES_CLOSED_ARCHIVE_DIR", ""))
    ap.add_argument("--file_max_records", type=int, default=int(os.getenv("FILE_MAX_RECORDS", "500000")))

    ap.add_argument("--edge_dir", default=os.getenv("EDGE_STACK_V1_DIR", "/var/lib/trade/ml_models/edge_stack_v1"))
    ap.add_argument("--timeout_build_s", type=int, default=int(os.getenv("EDGE_STACK_BUILD_TIMEOUT_S", "1800")))
    ap.add_argument("--timeout_train_s", type=int, default=int(os.getenv("EDGE_STACK_TRAIN_TIMEOUT_S", "3600")))

    args = ap.parse_args(list(argv) if argv is not None else None)

    run_id = str(args.run_id).strip()
    if not run_id:
        run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    edge_dir = os.path.abspath(args.edge_dir)
    run_dir = os.path.join(edge_dir, "runs", run_id)
    os.makedirs(run_dir, exist_ok=True)

    out_dataset = os.path.join(run_dir, "edge_train.jsonl")
    out_quarantine = os.path.join(run_dir, "edge_quarantine.jsonl")
    out_feature_cols = os.path.join(run_dir, "feature_cols.json")
    out_dataset_report = os.path.join(run_dir, "edge_dataset_report.json")

    out_model = os.path.join(run_dir, "edge_stack_v1.joblib")
    out_train_report = os.path.join(run_dir, "edge_train_report.json")
    out_bundle_report = os.path.join(run_dir, "edge_bundle_report.json")

    now = now_ms()
    since_ms = int(now - int(args.window_hours) * 3600 * 1000)
    until_ms = int(now)

    bundle: dict[str, Any] = {
        "run_id": run_id,
        "started_ts_ms": now,
        "window_hours": int(args.window_hours),
        "since_ms": since_ms,
        "until_ms": until_ms,
        "paths": {
            "run_dir": run_dir,
            "dataset": out_dataset,
            "dataset_report": out_dataset_report,
            "quarantine": out_quarantine,
            "feature_cols": out_feature_cols,
            "model": out_model,
            "train_report": out_train_report,
            "bundle_report": out_bundle_report,
        },
        "status": "running",
    }

    # 1) Build dataset
    t0 = time.time()
    build_cmd = [
        sys.executable,
        "-m",
        "ml_analysis.tools.build_edge_stack_dataset_from_redis",
        "--redis_url",
        str(args.redis_url),
        "--signal_stream",
        str(args.signal_stream),
        "--closed_stream",
        str(args.closed_stream),
        "--signals_count",
        str(int(args.signals_count)),
        "--closes_count",
        str(int(args.closes_count)),
        "--since_ms",
        str(int(since_ms)),
        "--until_ms",
        str(int(until_ms)),
        "--y_min_r",
        str(float(args.y_min_r)),
        "--out_jsonl",
        out_dataset,
        "--emit_feature_cols_json",
        out_feature_cols,
        "--out_quarantine_jsonl",
        out_quarantine,
        "--out_report_json",
        out_dataset_report,
        "--file_fallback",
        str(int(args.file_fallback)),
        "--archive_lookback_days",
        str(int(args.archive_lookback_days)),
        "--signal_archive_dir",
        str(args.signal_archive_dir),
        "--closed_archive_dir",
        str(args.closed_archive_dir),
        "--file_max_records",
        str(int(args.file_max_records)),
    ]

    rc, so, se = _run(build_cmd, timeout_s=int(args.timeout_build_s))
    bundle["dataset_build"] = {
        "rc": rc,
        "stderr_tail": (se or "")[-8000:],
        "elapsed_s": round(time.time() - t0, 3),
    }
    if rc != 0:
        bundle["status"] = "fail_build"
        atomic_write_json(out_bundle_report, bundle)
        write_train_metrics(args.redis_url, "metrics:edge_stack_train:last", {"status": bundle["status"], "run_id": run_id})
        return 2

    try:
        dataset_report = json.loads(open(out_dataset_report, encoding="utf-8").read())
    except Exception:
        dataset_report = {}
    bundle["dataset_report"] = dataset_report

    val = validate_dataset_report(
        dataset_report,
        min_joined=int(args.min_joined),
        pos_rate_min=float(args.pos_rate_min),
        pos_rate_max=float(args.pos_rate_max),
    )
    bundle["dataset_validation"] = {"ok": val.ok, "reason": val.reason, "joined": val.joined, "pos_rate": val.pos_rate}
    if not val.ok:
        bundle["status"] = "fail_validate"
        atomic_write_json(out_bundle_report, bundle)
        write_train_metrics(
            args.redis_url,
            "metrics:edge_stack_train:last",
            {"status": bundle["status"], "run_id": run_id, "joined": val.joined, "pos_rate": val.pos_rate, "reason": val.reason},
        )
        return 3

    # 2) Train OOF model
    t1 = time.time()
    train_cmd = [
        sys.executable,
        "-m",
        "ml_analysis.tools.train_edge_stack_v1_oof",
        "--data_jsonl",
        out_dataset,
        "--out_model",
        out_model,
        "--run_id",
        run_id,
        "--n_splits",
        str(int(args.train_n_splits)),
        "--purge_ms",
        str(int(args.train_purge_ms)),
        "--embargo_ms",
        str(int(args.train_embargo_ms)),
        "--p_min",
        str(float(args.train_p_min)),
        "--feature_cols_json",
        out_feature_cols,
        "--lr_C",
        "0.01",
    ]
    rc2, so2, se2 = _run(train_cmd, timeout_s=int(args.timeout_train_s))
    train_report = _last_json_line(so2)
    bundle["train"] = {
        "rc": rc2,
        "stderr_tail": (se2 or "")[-8000:],
        "elapsed_s": round(time.time() - t1, 3),
    }
    bundle["train_report"] = train_report
    atomic_write_json(out_train_report, train_report)

    if rc2 != 0 or not train_report:
        bundle["status"] = "fail_train"
        atomic_write_json(out_bundle_report, bundle)
        write_train_metrics(args.redis_url, "metrics:edge_stack_train:last", {"status": bundle["status"], "run_id": run_id})
        return 4

    # 3) Publish artifacts (candidate; optional champion)
    champions_dir = os.path.join(edge_dir, "champions")
    os.makedirs(champions_dir, exist_ok=True)
    candidate_path = os.path.join(champions_dir, f"edge_stack_v1_{run_id}.joblib")
    atomic_copy(out_model, candidate_path)

    publish = {
        "candidate_path": candidate_path,
        "auto_promote": int(args.auto_promote),
    }

    if int(args.auto_promote) == 1:
        champion_path = os.path.join(champions_dir, "edge_stack_v1_champion.joblib")
        atomic_copy(out_model, champion_path)
        publish["champion_path"] = champion_path
        _redis_set_json(
            args.redis_url,
            "cfg:ml_confirm:edge_stack_v1:champion",
            {"run_id": run_id, "model_path": champion_path, "trained_ts_ms": now_ms(), "report": train_report.get("oof", {})},
            ttl_s=0,
        )
    else:
        _redis_set_json(
            args.redis_url,
            "cfg:ml_confirm:edge_stack_v1:candidate",
            {"run_id": run_id, "model_path": candidate_path, "trained_ts_ms": now_ms(), "report": train_report.get("oof", {})},
            ttl_s=0,
        )

    bundle["publish"] = publish
    bundle["status"] = "ok"
    bundle["finished_ts_ms"] = now_ms()

    atomic_write_json(out_bundle_report, bundle)

    # 4) Write SRE metrics (best-effort)
    oof_meta = (((train_report or {}).get("oof") or {}).get("meta") or {})
    metrics = {
        "status": bundle["status"],
        "run_id": run_id,
        "joined": val.joined,
        "pos_rate": val.pos_rate,
        "oof_meta_brier": oof_meta.get("brier"),
        "oof_meta_ece": oof_meta.get("ece"),
        "oof_meta_precision_top5pct": oof_meta.get("precision_top5pct"),
        "candidate_path": candidate_path,
        "auto_promote": int(args.auto_promote),
    }
    write_train_metrics(args.redis_url, "metrics:edge_stack_train:last", metrics)

    # Print bundle report path for logs
    print(json.dumps({"status": "ok", "run_id": run_id, "bundle_report": out_bundle_report}, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
