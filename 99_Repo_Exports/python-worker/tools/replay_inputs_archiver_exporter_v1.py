from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
# P56 Prometheus exporter for replay_inputs_archiver metrics stored in Redis hash.

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
METRICS_HASH = env_str("REPLAY_ARCHIVER_METRICS_HASH", "metrics:replay_inputs_archiver")
PORT = env_int("REPLAY_ARCHIVER_EXPORTER_PORT", 9139)

g_archived = Gauge("replay_inputs_archiver_archived_total", "Total archived replay inputs.")
g_error = Gauge("replay_inputs_archiver_error_total", "Archiver errors (counter-like).")
g_bad = Gauge("replay_inputs_archiver_bad_payload_total", "Bad payloads (counter-like).")
g_no_sid = Gauge("replay_inputs_archiver_no_sid_total", "Missing sid (counter-like).")
g_seen = Gauge("replay_inputs_archiver_seen_dedup_skipped_total", "Seen-id dedup skips (counter-like).")
g_last_run = Gauge("replay_inputs_archiver_last_run_ts_ms", "Last run timestamp (ms).")
g_stale = Gauge("replay_inputs_archiver_staleness_sec", "Seconds since last run.")


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

            archived = _to_int(data.get("archived_total"))
            err = _to_int(data.get("error_total"))
            bad = _to_int(data.get("bad_payload_total"))
            no_sid = _to_int(data.get("no_sid_total"))
            seen = _to_int(data.get("seen_dedup_skipped_total"))
            last_ts = _to_int(data.get("last_run_ts_ms"))

            now_ms = get_ny_time_millis()
            staleness = max(0, int((now_ms - last_ts) / 1000)) if last_ts else 10**9

            g_archived.set(archived)
            g_error.set(err)
            g_bad.set(bad)
            g_no_sid.set(no_sid)
            g_seen.set(seen)
            g_last_run.set(last_ts)
            g_stale.set(staleness)
        except Exception:
            pass
        time.sleep(5)


if __name__ == "__main__":
    main()
