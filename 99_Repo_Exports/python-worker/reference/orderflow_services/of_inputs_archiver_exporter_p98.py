#!/usr/bin/env python3
"""Prometheus exporter for OFInputs DLQ/quarantine DB archiver status (P98).

Reads Redis hashes written by of_inputs_dlq_archive_to_db_p98.py:
  - metrics:of_inputs_dlq_db_archive
  - metrics:of_inputs_quarantine_db_archive

Exposes:
  of_inputs_archiver_last_run_ts_ms{kind="dlq|quarantine"}
  of_inputs_archiver_staleness_sec{kind=...}
  of_inputs_archiver_last_stream_ts_ms{kind=...}
  of_inputs_archiver_inserted_total{kind=...}
  of_inputs_archiver_error_total{kind=...}

Run:
  python -m orderflow_services.of_inputs_archiver_exporter_p98

ENV:
  REDIS_URL (default redis://redis-worker-1:6379/0)
  OF_INPUTS_ARCHIVER_EXPORTER_PORT (default 9156)
  OF_INPUTS_DLQ_DB_ARCHIVE_METRICS_KEY (default metrics:of_inputs_dlq_db_archive)
  OF_INPUTS_QUARANTINE_DB_ARCHIVE_METRICS_KEY (default metrics:of_inputs_quarantine_db_archive)
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
from typing import Any, Dict

from prometheus_client import Gauge, start_http_server  # type: ignore

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore


GAUGE_LAST_RUN_TS_MS = Gauge(
    "of_inputs_archiver_last_run_ts_ms"
    "Timestamp of last archiver run in milliseconds since epoch"
    ["kind"]
)
GAUGE_STALENESS_SEC = Gauge(
    "of_inputs_archiver_staleness_sec"
    "Seconds elapsed since last archiver run"
    ["kind"]
)
GAUGE_LAST_STREAM_TS_MS = Gauge(
    "of_inputs_archiver_last_stream_ts_ms"
    "Timestamp extracted from last processed stream ID (ms)"
    ["kind"]
)
GAUGE_INSERTED_TOTAL = Gauge(
    "of_inputs_archiver_inserted_total"
    "Cumulative rows inserted (monotonic-ish gauge from Redis hash)"
    ["kind"]
)
GAUGE_ERROR_TOTAL = Gauge(
    "of_inputs_archiver_error_total"
    "Cumulative error count (monotonic-ish gauge from Redis hash)"
    ["kind"]
)


def _i(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, (bytes, bytearray)):
            x = x.decode("utf-8", "replace")
        return int(float(x))
    except Exception:
        return default


def _s(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", "replace")
        except Exception:
            return ""
    return str(x)


def _stream_id_to_ts_ms(stream_id: str) -> int:
    try:
        return int(stream_id.split("-", 1)[0])
    except Exception:
        return 0


class Exporter:
    def __init__(self) -> None:
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.port = int(os.getenv("OF_INPUTS_ARCHIVER_EXPORTER_PORT", "9156") or 9156)
        self.key_dlq = os.getenv("OF_INPUTS_DLQ_DB_ARCHIVE_METRICS_KEY", "metrics:of_inputs_dlq_db_archive")
        self.key_quarantine = os.getenv(
            "OF_INPUTS_QUARANTINE_DB_ARCHIVE_METRICS_KEY", "metrics:of_inputs_quarantine_db_archive"
        )
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=False) if redis else None

    def _hgetall(self, key: str) -> Dict[str, Any]:
        if not self.r:
            return {}
        try:
            raw = self.r.hgetall(key) or {}
            out: Dict[str, Any] = {}
            for k, v in raw.items():
                out[_s(k)] = v
            return out
        except Exception:
            return {}

    def _emit(self, kind: str, d: Dict[str, Any]) -> None:
        last_run = _i(d.get("last_run_ts_ms"), 0)
        last_stream_id = _s(d.get("last_stream_id"))
        inserted_total = _i(d.get("inserted_total"), 0)
        error_total = _i(d.get("error_total"), 0)

        GAUGE_LAST_RUN_TS_MS.labels(kind=kind).set(last_run)
        if last_run > 0:
            GAUGE_STALENESS_SEC.labels(kind=kind).set(max(0.0, (get_ny_time_millis() - last_run) / 1000.0))
        else:
            GAUGE_STALENESS_SEC.labels(kind=kind).set(0)

        GAUGE_LAST_STREAM_TS_MS.labels(kind=kind).set(_stream_id_to_ts_ms(last_stream_id) if last_stream_id else 0)
        GAUGE_INSERTED_TOTAL.labels(kind=kind).set(inserted_total)
        GAUGE_ERROR_TOTAL.labels(kind=kind).set(error_total)

    def tick(self) -> None:
        self._emit("dlq", self._hgetall(self.key_dlq))
        self._emit("quarantine", self._hgetall(self.key_quarantine))


def main() -> None:
    ex = Exporter()
    start_http_server(ex.port)
    print(f"of_inputs_archiver_exporter_p98 serving on :{ex.port}")
    while True:
        ex.tick()
        time.sleep(5)


if __name__ == "__main__":
    main()
