#!/usr/bin/env python3
# Prometheus exporter for P55 close_backfill_replay_v1 metrics stored in Redis hash.
import os
import time
from typing import Any

import redis
from prometheus_client import Gauge, start_http_server

from utils.time_utils import get_ny_time_millis


def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None else v


def env_int(name: str, default: int) -> int:
    try:
        return int(env_str(name, str(default)).strip())
    except Exception:
        return default


REDIS_URL = env_str("REDIS_URL", "redis://localhost:6379/0")
METRICS_HASH = env_str("BACKFILL_METRICS_HASH", "metrics:close_backfill_replay")
PORT = env_int("BACKFILL_EXPORTER_PORT", 9138)

g_processed = Gauge("close_backfill_processed_total", "Backfill scanned events.")
g_close = Gauge("close_backfill_close_events_total", "Backfill found POSITION_CLOSED.")
g_direct = Gauge("close_backfill_direct_joined_total", "Backfill direct joined into trades:closed.")
g_wait = Gauge("close_backfill_pushed_to_close_wait_total", "Backfill pushed into trades:close_wait.")
g_bad = Gauge("close_backfill_bad_payload_total", "Bad payloads.")
g_no_sid = Gauge("close_backfill_no_sid_total", "Close events missing sid.")
g_joined = Gauge("close_backfill_already_joined_total", "Already joined skipped.")
g_seen_dedup = Gauge("close_backfill_seen_dedup_skipped_total", "Seen-event dedup skipped.")
g_last = Gauge("close_backfill_last_run_ts_ms", "Last run timestamp (ms).")
g_stale = Gauge("close_backfill_staleness_sec", "Seconds since last run.")


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
            raw: dict[bytes, bytes] = r.hgetall(METRICS_HASH) or {}
            data = {k.decode("utf-8", "replace"): v for k, v in raw.items()}
            processed = _to_int(data.get("processed"))
            direct = _to_int(data.get("direct_joined_total"))
            wait = _to_int(data.get("pushed_to_close_wait_total"))
            bad = _to_int(data.get("bad_payload_total"))
            no_sid = _to_int(data.get("no_sid_total"))
            joined = _to_int(data.get("already_joined_total"))
            seen_dedup = _to_int(data.get("seen_dedup_skipped_total"))
            last_ts = _to_int(data.get("last_run_ts_ms"))
            now_ms = get_ny_time_millis()
            staleness = max(0, int((now_ms - last_ts) / 1000)) if last_ts else 10**9

            g_processed.set(processed)
            g_direct.set(direct)
            g_wait.set(wait)
            g_bad.set(bad)
            g_no_sid.set(no_sid)
            g_joined.set(joined)
            g_seen_dedup.set(seen_dedup)
            g_last.set(last_ts)
            g_stale.set(staleness)
        except Exception:
            pass
        time.sleep(5)


if __name__ == "__main__":
    main()
