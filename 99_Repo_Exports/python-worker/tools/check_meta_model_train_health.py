from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
from typing import Any, Dict

import redis


def now_ms() -> int:
    return get_ny_time_millis()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--max_train_stale_ms", type=int, default=int(os.getenv("META_MAX_TRAIN_STALE_MS", "21600000")))  # 6h
    ap.add_argument("--max_apply_stale_ms", type=int, default=int(os.getenv("META_MAX_APPLY_STALE_MS", "86400000")))  # 24h
    ap.add_argument("--print_json", action="store_true")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    ts_train = int(r.get("meta_model:last_train_ts_ms") or 0)
    ts_apply = int(r.get("meta_model:last_apply_ts_ms") or 0)
    status = str(r.get("meta_model:last_status") or "")
    report_raw = r.get("meta_model:last_train_report") or "{}"
    try:
        report = json.loads(report_raw)
    except Exception:
        report = {"raw": report_raw}

    now = now_ms()
    out: Dict[str, Any] = {
        "now_ms": now,
        "train_ts_ms": ts_train,
        "apply_ts_ms": ts_apply,
        "status": status,
        "train_stale_ms": (now - ts_train) if ts_train else None,
        "apply_stale_ms": (now - ts_apply) if ts_apply else None,
        "report": report,
        "alerts": [],
    }

    if not ts_train or (now - ts_train) > args.max_train_stale_ms:
        out["alerts"].append("train_stale")
    if ts_apply and (now - ts_apply) > args.max_apply_stale_ms:
        out["alerts"].append("apply_stale")
    if status.startswith("err:"):
        out["alerts"].append("train_error")
    if status.startswith("fail:"):
        out["alerts"].append("train_gate_fail")

    if args.print_json:
        print(json.dumps(out, ensure_ascii=False, sort_keys=True))

    # exit code: 0 OK, 2 FAIL
    raise SystemExit(2 if out["alerts"] else 0)


if __name__ == "__main__":
    main()
