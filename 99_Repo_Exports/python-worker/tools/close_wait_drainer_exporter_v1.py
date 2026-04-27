from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
# Prometheus exporter for P54 close-wait drainer metrics stored in Redis hash.

import os
import time
from typing import Any, Dict

import redis
from prometheus_client import Gauge, start_http_server


def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None else v


def env_int(name: str, default: int) -> int:
    try:
        return int(env_str(name, str(default)).strip())
    except Exception:
        return default


REDIS_URL = env_str("REDIS_URL", "redis://localhost:6379/0")
METRICS_HASH = env_str("CLOSE_WAIT_METRICS_HASH", "metrics:close_wait_drainer")
PORT = env_int("CLOSE_WAIT_EXPORTER_PORT", 9137)

g_seen = Gauge("close_wait_seen_total", "Close-wait messages seen (counter-like gauge).")
g_joined = Gauge("close_wait_joined_total", "Close-wait messages successfully joined.")
g_missing = Gauge("close_wait_missing_decision_total", "Close-wait messages missing decision.")
g_dead = Gauge("close_wait_dead_letter_total", "Dead-lettered messages.")
g_err = Gauge("close_wait_error_total", "Errors.")
g_dedup = Gauge("close_wait_dedup_skipped_total", "Dedup skipped.")
g_dedup_race = Gauge("close_wait_dedup_race_skipped_total", "Dedup race skipped.")
g_lock = Gauge("close_wait_lock_contended_total", "Lock contended.")
g_pending = Gauge("close_wait_pending_count", "Pending entries in group.")
g_last = Gauge("close_wait_last_run_ts_ms", "Last run timestamp (ms).")
g_stale = Gauge("close_wait_staleness_sec", "Seconds since last run.")


def _to_int(v: Any) -> int:
    try:
        if v is None:
            return 0
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "replace")
        return int(float(str(v)))
    except Exception:
        return 0


def main() -> None:
    r = redis.Redis.from_url(REDIS_URL, decode_responses=False)
    start_http_server(PORT)
    while True:
        try:
            raw: Dict[bytes, bytes] = r.hgetall(METRICS_HASH) or {}
            data = {k.decode("utf-8", "replace"): v for k, v in raw.items()}
            seen = _to_int(data.get("seen_total"))
            joined = _to_int(data.get("joined_total"))
            missing = _to_int(data.get("missing_decision_total"))
            dead = _to_int(data.get("dead_letter_total"))
            err = _to_int(data.get("error_total"))
            dedup = _to_int(data.get("dedup_skipped_total"))
            dedup_race = _to_int(data.get("dedup_race_skipped_total"))
            lockc = _to_int(data.get("lock_contended_total"))
            pending = _to_int(data.get("pending_count"))
            last_ts = _to_int(data.get("last_run_ts_ms"))
            now_ms = get_ny_time_millis()
            staleness = max(0, int((now_ms - last_ts) / 1000)) if last_ts else 10**9

            g_seen.set(seen)
            g_joined.set(joined)
            g_missing.set(missing)
            g_dead.set(dead)
            g_err.set(err)
            g_dedup.set(dedup)
            g_dedup_race.set(dedup_race)
            g_lock.set(lockc)
            g_pending.set(pending)
            g_last.set(last_ts)
            g_stale.set(staleness)
        except Exception:
            pass
        time.sleep(5)


if __name__ == "__main__":
    main()
