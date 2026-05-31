import json
import os
import sys
import time

import redis

from utils.time_utils import get_ny_time_millis

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# Thresholds (can be overridden by ENV)
MAX_STALE_MS = int(os.getenv("NOTIFY_SRE_MAX_STALE_MS", "300000"))  # 5 min
MAX_QUEUE_LAG_MS = int(os.getenv("NOTIFY_SRE_MAX_QUEUE_LAG_MS", "600000")) # 10 min
MAX_ERR_RATE = float(os.getenv("NOTIFY_SRE_MAX_ERR_RATE", "0.20"))  # 20%
MIN_SAMPLES = int(os.getenv("NOTIFY_SRE_MIN_SAMPLES", "10"))
MAX_PENDING = int(os.getenv("NOTIFY_SRE_MAX_PENDING", "1000"))

def get_redis_client():
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)

def check_health():
    r = get_redis_client()
    now_ms = get_ny_time_millis()
    current_bucket = int(time.time() / 60)

    health_data = {
        "status": "ok",
        "issues": [],
        "metrics": {}
    }

    # 1. Check Stale OK
    last_ok_str = r.get("notify:last_ok_ts_ms")
    last_ok_ms = int(last_ok_str) if last_ok_str else 0
    time_since_ok = now_ms - last_ok_ms

    health_data["metrics"]["time_since_last_ok_ms"] = time_since_ok
    # We delay staleness check logic to combine with lag/pending

    # 2. Check Queue Lag
    last_lag_str = r.get("notify:last_queue_lag_ms")
    last_lag_ms = int(last_lag_str) if last_lag_str else 0
    health_data["metrics"]["queue_lag_ms"] = last_lag_ms

    if last_lag_ms > MAX_QUEUE_LAG_MS:
        health_data["status"] = "crit"
        health_data["issues"].append(f"Queue lag {last_lag_ms}ms > {MAX_QUEUE_LAG_MS}ms")

    # If lag is high AND no OK recently -> definitely stalled.
    if time_since_ok > MAX_STALE_MS and last_lag_ms > 1000:
        health_data["status"] = "crit"
        health_data["issues"].append(f"Stalled: No success for {time_since_ok}ms and lag is {last_lag_ms}ms")

    # 3. Check Pending
    # Implies we track total pending somewhere or just check key
    # (The implementation plan says notify:last_pending_n)
    pending_str = r.get("notify:last_pending_n")
    pending_n = int(pending_str) if pending_str else 0
    health_data["metrics"]["pending_n"] = pending_n

    if pending_n > MAX_PENDING:
        health_data["status"] = "crit"
        health_data["issues"].append(f"Pending messages {pending_n} > {MAX_PENDING}")

    # 4. Check Error Rate (Window 5m)
    total_ok = 0
    total_err = 0

    # Check last 5 minutes (buckets)
    for i in range(5):
        b = current_bucket - i
        key = f"notify:win5m:{b}"
        # We assume HGETALL returns {ok: "N", err: "M"}
        stats = r.hgetall(key)
        if stats:
            total_ok += int(stats.get("ok", 0))
            total_err += int(stats.get("err", 0))

    total = total_ok + total_err
    error_rate = 0.0
    if total > 0:
        error_rate = total_err / total

    health_data["metrics"]["error_rate_5m"] = round(error_rate, 4)
    health_data["metrics"]["samples_5m"] = total

    if total >= MIN_SAMPLES and error_rate > MAX_ERR_RATE:
        health_data["status"] = "crit"
        health_data["issues"].append(f"High error rate {error_rate:.2%} (threshold {MAX_ERR_RATE:.0%})")

    return health_data

if __name__ == "__main__":
    try:
        data = check_health()
        print(json.dumps(data))
        if data["status"] != "ok":
            sys.exit(2)
        sys.exit(0)
    except Exception as e:
        print(json.dumps({"status": "err", "error": str(e), "issues": [f"Exception: {str(e)}"]}))
        sys.exit(1)
