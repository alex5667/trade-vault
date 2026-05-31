from __future__ import annotations

#!/usr/bin/env python3
from utils.time_utils import get_ny_time_millis

"""Prometheus exporter for OFC contextual writer + ops bundle health.

Reads Redis hashes written by:
- services/orderflow/ofc_contextual_decision_writer_v1.py
- orderflow_services/nightly_ofc_contextual_ops_bundle_v1.py,
""",
import os
import time
from typing import Any

from prometheus_client import Gauge, start_http_server  # type: ignore

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore

WRITER_LAST_RUN_TS_MS = Gauge("ofc_contextual_writer_last_run_ts_ms", "Last writer run timestamp in ms", [])
WRITER_STALENESS_SEC = Gauge("ofc_contextual_writer_staleness_sec", "Seconds since OFC contextual writer last run", [])
WRITER_WRITTEN_TOTAL = Gauge("ofc_contextual_writer_written_total", "Writer cumulative rows written (monotonic-ish from Redis hash)", [])
WRITER_DB_FAIL_TOTAL = Gauge("ofc_contextual_writer_db_fail_total", "Writer cumulative DB failures (monotonic-ish from Redis hash)", [])
WRITER_DLQ_TOTAL = Gauge("ofc_contextual_writer_dlq_total", "Writer cumulative DLQ rows (monotonic-ish from Redis hash)", [])
WRITER_PENDING_COUNT = Gauge("ofc_contextual_writer_pending_count", "Writer current pending entries in Redis consumer group", [])
WRITER_LAST_OK = Gauge("ofc_contextual_writer_last_ok", "1 if writer last batch succeeded", [])
WRITER_LAST_BATCH_ROWS = Gauge("ofc_contextual_writer_last_batch_rows", "Rows written in last successful batch", [])

OPS_LAST_RUN_TS_MS = Gauge("ofc_contextual_ops_last_run_ts_ms", "Last OFC contextual ops bundle run timestamp in ms", [])
OPS_STALENESS_SEC = Gauge("ofc_contextual_ops_staleness_sec", "Seconds since OFC contextual ops bundle last run", [])
OPS_LAST_OK = Gauge("ofc_contextual_ops_last_ok", "1 if last OFC contextual ops bundle run succeeded", [])
OPS_LAST_EXIT_CODE = Gauge("ofc_contextual_ops_last_exit_code", "Last OFC contextual ops bundle exit code", [])
BUNDLE_CREATED_TS_MS = Gauge("ofc_contextual_bundle_created_ts_ms", "Active OFC contextual bundle creation timestamp in ms", [])
BUNDLE_AGE_SECONDS = Gauge("ofc_contextual_bundle_age_seconds", "Age of active OFC contextual bundle in seconds", [])


def _i(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, (bytes, bytearray)):
            x = x.decode("utf-8", "replace")
        return int(float(x))
    except Exception:
        return default


class Exporter:
    def __init__(self) -> None:
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.port = int(os.getenv("OFC_CTX_EXPORTER_PORT", "9159") or 9159)
        self.writer_key = os.getenv("OFC_CTX_DECISION_METRICS_KEY", "metrics:ofc_contextual_decision_writer")
        self.ops_key = os.getenv("OFC_CTX_OPS_METRICS_KEY", "metrics:ofc_contextual_ops_bundle")
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=False) if redis else None

    def _hgetall(self, key: str) -> dict[str, Any]:
        if not self.r:
            return {}
        try:
            raw = self.r.hgetall(key) or {}
            return {
                (k.decode("utf-8", "replace") if isinstance(k, (bytes, bytearray)) else str(k)): v
                for k, v in raw.items()
            }
        except Exception:
            return {}

    def tick(self) -> None:
        now_ms = get_ny_time_millis()
        w = self._hgetall(self.writer_key)
        w_last = _i(w.get("last_run_ts_ms"), 0)
        WRITER_LAST_RUN_TS_MS.set(w_last)
        WRITER_STALENESS_SEC.set(max(0.0, (now_ms - w_last) / 1000.0) if w_last > 0 else 0.0)
        WRITER_WRITTEN_TOTAL.set(_i(w.get("written_total"), 0))
        WRITER_DB_FAIL_TOTAL.set(_i(w.get("db_fail_total"), 0))
        WRITER_DLQ_TOTAL.set(_i(w.get("dlq_total"), 0))
        WRITER_PENDING_COUNT.set(_i(w.get("pending_count"), 0))
        WRITER_LAST_OK.set(_i(w.get("last_ok"), 0))
        WRITER_LAST_BATCH_ROWS.set(_i(w.get("last_batch_rows"), 0))

        o = self._hgetall(self.ops_key)
        o_last = _i(o.get("last_run_ts_ms"), 0)
        created_ts_ms = _i(o.get("bundle_created_ts_ms"), 0)
        OPS_LAST_RUN_TS_MS.set(o_last)
        OPS_STALENESS_SEC.set(max(0.0, (now_ms - o_last) / 1000.0) if o_last > 0 else 0.0)
        OPS_LAST_OK.set(_i(o.get("last_ok"), 0))
        OPS_LAST_EXIT_CODE.set(_i(o.get("last_exit_code"), 0))
        BUNDLE_CREATED_TS_MS.set(created_ts_ms)
        BUNDLE_AGE_SECONDS.set(max(0.0, (now_ms - created_ts_ms) / 1000.0) if created_ts_ms > 0 else 0.0)


def run() -> int:
    port = int(os.getenv("OFC_CTX_EXPORTER_PORT", "9159") or 9159)
    start_http_server(port)
    print(f"ofc_contextual_exporter_v1 serving on :{port}")
    ex = Exporter()
    while True:
        ex.tick()
        time.sleep(5.0)


if __name__ == "__main__":
    raise SystemExit(run())
