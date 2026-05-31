#!/usr/bin/env python3
# python-worker/tools/check_tb_health.py
from __future__ import annotations

import argparse
import json
from typing import Any

from utils.time_utils import get_ny_time_millis

try:
    import redis  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit("redis package is required") from e


def _get_int(r: redis.Redis, key: bytes) -> int:
    try:
        v = r.get(key)
        if v is None:
            return 0
        if isinstance(v, bytes):
            v = v.decode(errors="ignore")
        return int(float(v))
    except Exception:
        return 0


def _zset_oldest(r: redis.Redis, key: str) -> tuple[str, int]:
    try:
        rows = r.zrange(key, 0, 0, withscores=True)
        if not rows:
            return ("", 0)
        job_id, score = rows[0]
        if isinstance(job_id, bytes):
            job_id = job_id.decode(errors="ignore")
        return (str(job_id), int(score))
    except Exception:
        return ("", 0)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_url", required=True)
    ap.add_argument("--max_input_lag_ms", type=int, default=120000)
    ap.add_argument("--max_label_lag_ms", type=int, default=300000)
    ap.add_argument("--max_jobs", type=int, default=200000)
    ap.add_argument("--jobs_zset", default="tb:jobs:zset")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=False)
    now_ms = get_ny_time_millis()

    last_ts_ms = _get_int(r, b"tb:last_ts_ms")
    last_label_ts_ms = _get_int(r, b"tb:last_label_ts_ms")
    last_err_ts_ms = _get_int(r, b"tb:last_err_ts_ms")

    input_lag_ms = now_ms - last_ts_ms if last_ts_ms > 0 else 10**12
    label_lag_ms = now_ms - last_label_ts_ms if last_label_ts_ms > 0 else 10**12

    try:
        jobs = int(r.zcard(args.jobs_zset))
    except Exception:
        jobs = -1

    oldest_job_id, oldest_due_ms = _zset_oldest(r, args.jobs_zset)
    oldest_overdue_ms = max(0, now_ms - oldest_due_ms) if oldest_due_ms > 0 else 0

    ok = True
    reasons = []

    if input_lag_ms > args.max_input_lag_ms:
        ok = False
        reasons.append(f"input_lag_ms={input_lag_ms}")

    if label_lag_ms > args.max_label_lag_ms:
        ok = False
        reasons.append(f"label_lag_ms={label_lag_ms}")

    if jobs >= 0 and jobs > args.max_jobs:
        ok = False
        reasons.append(f"jobs_backlog={jobs}")

    # also treat very overdue oldest job as warning
    if oldest_overdue_ms > max(args.max_label_lag_ms, 600000):
        ok = False
        reasons.append(f"oldest_overdue_ms={oldest_overdue_ms}")

    out: dict[str, Any] = {
        "ok": ok,
        "now_ms": now_ms,
        "input_lag_ms": input_lag_ms,
        "label_lag_ms": label_lag_ms,
        "jobs": jobs,
        "oldest_job_id": oldest_job_id,
        "oldest_overdue_ms": oldest_overdue_ms,
        "last_err_age_ms": (now_ms - last_err_ts_ms) if last_err_ts_ms > 0 else None,
        "reasons": reasons,
    }
    print(json.dumps(out, ensure_ascii=False))
    raise SystemExit(0 if ok else 2)


if __name__ == "__main__":  # pragma: no cover
    main()
