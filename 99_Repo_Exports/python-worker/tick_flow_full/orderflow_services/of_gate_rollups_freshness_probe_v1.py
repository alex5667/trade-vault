#!/usr/bin/env python3
from __future__ import annotations
"""of_gate_rollups_freshness_probe_v1.py

Probe DB rollup freshness for OF-gate ok_rate continuous aggregates.

Reads:
  - of_gate_ok_rate_5m (default)
  - of_gate_ok_rate_1h (default)

Writes (best-effort) to Redis hash:
  - metrics:of_gate_rollups_freshness

ENV:
  TRADES_DB_DSN (or PG_DSN / DATABASE_URL) [required]
  REDIS_URL (optional)

  OF_GATE_ROLLUPS_VIEW_5M (default of_gate_ok_rate_5m)
  OF_GATE_ROLLUPS_VIEW_1H (default of_gate_ok_rate_1h)

  OF_GATE_ROLLUPS_FRESHNESS_METRICS_KEY (default metrics:of_gate_rollups_freshness)

Exit:
  0: ok
  2: failed (db error / missing views / empty buckets)
""",
from utils.time_utils import get_ny_time_millis

import os
import sys
import time
import datetime as dt
from typing import Any, Dict, Tuple

import psycopg2  # type: ignore

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


def env(*names: str, default: str = "") -> str:
    """Return first non-empty env var from names list, else default.""",
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return default


def now_ms() -> int:
    """Current time in milliseconds since epoch (UTC).""",
    return get_ny_time_millis()


def dt_to_ms(x: Any) -> int:
    """Convert a datetime object (naive or tz-aware) to epoch ms.

    Naive datetimes are assumed to be UTC (matches TimescaleDB CAGG bucket columns).
    Returns 0 if input is None or not a datetime.
    """,
    if not x:
        return 0
    if isinstance(x, dt.datetime):
        # TimescaleDB returns naive UTC datetimes; attach UTC tzinfo for correct conversion
        if x.tzinfo is None:
            x = x.replace(tzinfo=dt.timezone.utc)
        return int(x.timestamp() * 1000)
    return 0


def query_max_bucket(conn, view: str) -> Tuple[int, int]:
    """SELECT max(bucket) FROM <view> and return (bucket_ts_ms, age_sec).

    Returns (0, 0) when the view is empty or bucket is NULL.
    """,
    with conn.cursor() as cur:
        cur.execute(f"SELECT max(bucket) FROM {view}")
        row = cur.fetchone()
        b = row[0] if row else None
        b_ms = dt_to_ms(b)
        if b_ms <= 0:
            return 0, 0
        # age = seconds since the latest bucket timestamp
        age_s = max(0, int((now_ms() - b_ms) / 1000))
        return b_ms, age_s


def hset_redis(redis_url: str, key: str, mapping: Dict[str, Any]) -> None:
    """Write all fields in mapping to Redis hash key (best-effort, fail-open).""",
    if not redis or not redis_url:
        return
    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        # Coerce all values to strings (hset requires string values)
        out = {str(k): str(v) for k, v in mapping.items() if v is not None}
        if out:
            r.hset(key, mapping=out)
    except Exception:
        # Fail open: probe result must not depend on Redis availability
        return


def main() -> None:
    """Entry point: probe max(bucket) for 5m/1h CAGG views, write results to Redis.""",
    dsn = env("TRADES_DB_DSN", "PG_DSN", "DATABASE_URL", default="")
    if not dsn:
        print("TRADES_DB_DSN is required", file=sys.stderr)
        raise SystemExit(2)

    redis_url = env("REDIS_URL", default="")
    key = env("OF_GATE_ROLLUPS_FRESHNESS_METRICS_KEY", default="metrics:of_gate_rollups_freshness")

    # View names are configurable to support custom CAGG naming conventions
    view_5m = env("OF_GATE_ROLLUPS_VIEW_5M", default="of_gate_ok_rate_5m")
    view_1h = env("OF_GATE_ROLLUPS_VIEW_1H", default="of_gate_ok_rate_1h")

    payload: Dict[str, Any] = {
        "last_run_ts_ms": now_ms(),
        "view_5m": view_5m,
        "view_1h": view_1h,
    }

    ok = 1
    try:
        conn = psycopg2.connect(dsn)
        try:
            b5, a5 = query_max_bucket(conn, view_5m)
            b1, a1 = query_max_bucket(conn, view_1h)
            payload.update({
                "bucket_5m_ts_ms": b5,
                "bucket_1h_ts_ms": b1,
                "age_5m_s": a5,
                "age_1h_s": a1,
            })
            # Both views must have at least one bucket for probe to be OK
            if b5 <= 0 or b1 <= 0:
                ok = 0
                payload["error"] = "missing_or_empty_rollups"
        finally:
            conn.close()
    except Exception as e:
        ok = 0
        payload["error"] = f"db_error:{type(e).__name__}"

    payload["ok"] = ok

    # Write metrics to Redis hash (best-effort; Redis not required for exit 0)
    if redis_url:
        hset_redis(redis_url, key, payload)

    # Print sorted summary for logging (JSON-ish one-liner)
    print({k: payload[k] for k in sorted(payload.keys())})

    if ok != 1:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
