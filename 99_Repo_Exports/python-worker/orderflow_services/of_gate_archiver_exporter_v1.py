#!/usr/bin/env python3
from __future__ import annotations
"""of_gate_archiver_exporter_v1.py

Prometheus exporter (P78) for OF-gate archiver + quarantine archiver + rollups refresh status.

Sources (Redis hashes written by stream_archiver.py and of_gate_history_migration_v1.py):
  - metrics:of_gate_metrics_archiver       — metrics:of_gate archiver status
  - metrics:of_gate_quarantine_archiver    — quarantine:metrics:of_gate archiver status
  - metrics:of_gate_rollups_refresh        — CAGG refresh job status

Metrics exposed (all with label `kind` in {metrics, quarantine, rollups_refresh}):
  of_gate_archiver_last_run_ts_ms   — timestamp of last successful run (ms)
  of_gate_archiver_staleness_sec    — seconds since last run
  of_gate_archiver_last_stream_ts_ms — ms extracted from last_stream_id field
  of_gate_archiver_inserted_total   — cumulative rows inserted (monotonic-ish gauge)
  of_gate_archiver_error_total      — cumulative error count

Run:
  python -m orderflow_services.of_gate_archiver_exporter_v1

ENV:
  REDIS_URL (default redis://redis-worker-1:6379/0)
  OF_GATE_ARCHIVER_EXPORTER_PORT (default 9152)
  OF_GATE_ARCHIVER_METRICS_KEY (default metrics:of_gate_metrics_archiver)
  OF_GATE_QUARANTINE_ARCHIVER_METRICS_KEY (default metrics:of_gate_quarantine_archiver)
  OF_GATE_ROLLUPS_REFRESH_METRICS_KEY (default metrics:of_gate_rollups_refresh)

Notes:
  - fail-open if redis client import missing
  - tick loop every 5s; blocking HGETALL
"""

from utils.time_utils import get_ny_time_millis

import os
import time
from typing import Any, Dict

from prometheus_client import Gauge, start_http_server  # type: ignore

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


# Prometheus gauge families — one label `kind` to differentiate archiver types
GAUGE_LAST_RUN_TS_MS = Gauge(
    "of_gate_archiver_last_run_ts_ms",
    "Timestamp of last archiver run in milliseconds since epoch",
    ["kind"],
)
GAUGE_STALENESS_SEC = Gauge(
    "of_gate_archiver_staleness_sec",
    "Seconds elapsed since last archiver run",
    ["kind"],
)
GAUGE_LAST_STREAM_TS_MS = Gauge(
    "of_gate_archiver_last_stream_ts_ms",
    "Timestamp extracted from last processed stream ID (ms)",
    ["kind"],
)
GAUGE_INSERTED_TOTAL = Gauge(
    "of_gate_archiver_inserted_total",
    "Cumulative rows inserted (monotonic-ish gauge from Redis hash)",
    ["kind"],
)
GAUGE_ERROR_TOTAL = Gauge(
    "of_gate_archiver_error_total",
    "Cumulative error count (monotonic-ish gauge from Redis hash)",
    ["kind"],
)

# P80: Rollups freshness probe gauges (written by of_gate_rollups_freshness_probe_v1)
GAUGE_ROLLUPS_BUCKET_AGE_SEC = Gauge(
    "of_gate_rollups_bucket_age_sec",
    "Seconds since the latest rollups CAGG bucket",
    ["view"],
)
GAUGE_ROLLUPS_BUCKET_TS_MS = Gauge(
    "of_gate_rollups_bucket_ts_ms",
    "Latest rollups CAGG bucket timestamp (ms since epoch)",
    ["view"],
)
GAUGE_ROLLUPS_FRESHNESS_OK = Gauge(
    "of_gate_rollups_freshness_ok",
    "1 if rollups freshness probe ran successfully and found data in both 5m/1h views",
    [],
)

# P81: Timescale policy probe gauges (written by of_gate_timescale_policy_probe_v1)
GAUGE_TS_PRESENT = Gauge("of_gate_timescale_present", "1 if timescaledb extension present", [])
GAUGE_TS_EXPECT = Gauge("of_gate_timescale_expect", "1 if timescaledb expected", [])
GAUGE_TS_POLICIES_MISSING = Gauge("of_gate_timescale_policies_missing", "count of missing required policies", [])
GAUGE_TS_POLICIES_DISABLED = Gauge("of_gate_timescale_policies_disabled", "count of disabled required policies", [])
GAUGE_TS_POLICY_PRESENT = Gauge("of_gate_timescale_policy_present", "policy present (1/0)", ["policy"])
GAUGE_TS_POLICY_DISABLED = Gauge("of_gate_timescale_policy_disabled", "policy disabled (1/0)", ["policy"])


def _i(x: Any, default: int = 0) -> int:
    """Safe int coercion from Redis hash value (may be bytes or str)."""
    try:
        if x is None:
            return default
        if isinstance(x, (bytes, bytearray)):
            x = x.decode("utf-8", "replace")
        return int(float(x))
    except Exception:
        return default


def _s(x: Any) -> str:
    """Safe str coercion from Redis hash value."""
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", "replace")
        except Exception:
            return ""
    return str(x)


def _stream_id_to_ts_ms(stream_id: str) -> int:
    """Extract timestamp ms from Redis stream ID '<ms>-<seq>'. Returns 0 on failure."""
    try:
        return int(stream_id.split("-", 1)[0])
    except Exception:
        return 0


class Exporter:
    def __init__(self) -> None:
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.port = int(os.getenv("OF_GATE_ARCHIVER_EXPORTER_PORT", "9152") or 9152)
        self.key_metrics = os.getenv("OF_GATE_ARCHIVER_METRICS_KEY", "metrics:of_gate_metrics_archiver")
        self.key_quarantine = os.getenv("OF_GATE_QUARANTINE_ARCHIVER_METRICS_KEY", "metrics:of_gate_quarantine_archiver")
        self.key_rollups = os.getenv("OF_GATE_ROLLUPS_REFRESH_METRICS_KEY", "metrics:of_gate_rollups_refresh")
        # P80: Key for freshness probe hash written by of_gate_rollups_freshness_probe_v1
        self.key_rollups_freshness = os.getenv(
            "OF_GATE_ROLLUPS_FRESHNESS_METRICS_KEY", "metrics:of_gate_rollups_freshness"
        )
        # P81: Key for timescale policy probe hash written by of_gate_timescale_policy_probe_v1
        self.key_ts_policies = os.getenv(
            "OF_GATE_TIMESCALE_POLICIES_METRICS_KEY", "metrics:of_gate_timescale_policies"
        )
        # fail-open: exporter works (returns zeros) even if redis library isn't installed
        self.r = redis.Redis.from_url(self.redis_url, decode_responses=False) if redis else None

    def _hgetall(self, key: str) -> Dict[str, Any]:
        """Read Redis hash, return empty dict on any error (fail-open)."""
        if not self.r:
            return {}
        try:
            raw = self.r.hgetall(key) or {}
            out: Dict[str, Any] = {}
            for k, v in raw.items():
                ks = _s(k)
                out[ks] = v
            return out
        except Exception:
            return {}

    def _emit(self, kind: str, d: Dict[str, Any]) -> None:
        """Update all gauge metrics for a given archiver kind from its Redis hash dict."""
        last_run = _i(d.get("last_run_ts_ms"), 0)
        last_stream_id = _s(d.get("last_stream_id"))
        inserted_total = _i(d.get("inserted_total"), 0)
        error_total = _i(d.get("error_total"), 0)

        GAUGE_LAST_RUN_TS_MS.labels(kind=kind).set(last_run)

        # Staleness: 0 if never run (don't produce misleading large values)
        if last_run > 0:
            GAUGE_STALENESS_SEC.labels(kind=kind).set(max(0, (get_ny_time_millis() - last_run) / 1000.0))
        else:
            GAUGE_STALENESS_SEC.labels(kind=kind).set(0)

        # Last stream timestamp from stream ID
        if last_stream_id:
            GAUGE_LAST_STREAM_TS_MS.labels(kind=kind).set(_stream_id_to_ts_ms(last_stream_id))
        else:
            GAUGE_LAST_STREAM_TS_MS.labels(kind=kind).set(0)

        GAUGE_INSERTED_TOTAL.labels(kind=kind).set(inserted_total)
        GAUGE_ERROR_TOTAL.labels(kind=kind).set(error_total)

    def _emit_rollups_freshness(self, d: Dict[str, Any]) -> None:
        """Update rollups freshness gauges from Redis hash written by the probe (P80).

        Hash fields written by of_gate_rollups_freshness_probe_v1:
          ok              – 1 if probe succeeded, 0 otherwise
          bucket_5m_ts_ms – max(bucket) for 5m CAGG in ms
          bucket_1h_ts_ms – max(bucket) for 1h CAGG in ms
          age_5m_s        – seconds since latest 5m bucket
          age_1h_s        – seconds since latest 1h bucket
          last_run_ts_ms  – probe run timestamp (used for staleness via kind=rollups_freshness)
        """
        try:
            ok = _i(d.get('ok'), 0)
            GAUGE_ROLLUPS_FRESHNESS_OK.set(1 if ok == 1 else 0)
            GAUGE_ROLLUPS_BUCKET_TS_MS.labels(view='5m').set(_i(d.get('bucket_5m_ts_ms'), 0))
            GAUGE_ROLLUPS_BUCKET_TS_MS.labels(view='1h').set(_i(d.get('bucket_1h_ts_ms'), 0))
            GAUGE_ROLLUPS_BUCKET_AGE_SEC.labels(view='5m').set(_i(d.get('age_5m_s'), 0))
            GAUGE_ROLLUPS_BUCKET_AGE_SEC.labels(view='1h').set(_i(d.get('age_1h_s'), 0))
        except Exception:
            # fail-open: never crash the exporter loop
            return

    def _emit_timescale_policies(self, d: Dict[str, Any]) -> None:
        """Update Timescale policy probe gauges from Redis hash (P81).

        Hash fields written by of_gate_timescale_policy_probe_v1:
          timescale_present   – 1 if extension found, 0 otherwise
          expect_timescale    – 1 if extension is expected (ENV gate)
          missing_count       – number of missing required policies
          disabled_count      – number of disabled required policies
          present_<policy>    – 1 if policy job found, 0 otherwise
          disabled_<policy>   – 1 if policy job is disabled, 0 otherwise
        """
        try:
            GAUGE_TS_PRESENT.set(1 if _i(d.get('timescale_present'), 0) == 1 else 0)
            GAUGE_TS_EXPECT.set(1 if _i(d.get('expect_timescale'), 1) == 1 else 0)
            GAUGE_TS_POLICIES_MISSING.set(_i(d.get('missing_count'), 0))
            GAUGE_TS_POLICIES_DISABLED.set(_i(d.get('disabled_count'), 0))

            policies = [
                'retention_of_gate_metrics',
                'retention_of_gate_metrics_quarantine',
                'refresh_of_gate_ok_rate_5m',
                'refresh_of_gate_ok_rate_1h',
            ]
            for p in policies:
                GAUGE_TS_POLICY_PRESENT.labels(policy=p).set(_i(d.get(f'present_{p}'), 0))
                GAUGE_TS_POLICY_DISABLED.labels(policy=p).set(_i(d.get(f'disabled_{p}'), 0))
        except Exception:
            # fail-open: never crash the exporter loop
            return

    def tick(self) -> None:
        """Read all Redis hashes and update Prometheus gauges."""
        self._emit("metrics", self._hgetall(self.key_metrics))
        self._emit("quarantine", self._hgetall(self.key_quarantine))
        self._emit("rollups_refresh", self._hgetall(self.key_rollups))
        # P80: Freshness probe – emit rollups_freshness staleness via general track
        freshness_d = self._hgetall(self.key_rollups_freshness)
        self._emit_rollups_freshness(freshness_d)
        # Also expose probe last_run_ts_ms under kind='rollups_freshness' for staleness alert
        self._emit("rollups_freshness", freshness_d)
        # P81: Timescale policy probe – emit staleness + per-policy gauges
        ts_policies_d = self._hgetall(self.key_ts_policies)
        self._emit("timescale_policy_probe", ts_policies_d)
        self._emit_timescale_policies(ts_policies_d)


def main() -> None:
    ex = Exporter()
    start_http_server(ex.port)
    print(f"of_gate_archiver_exporter_v1 serving on :{ex.port}")
    while True:
        ex.tick()
        time.sleep(5)


if __name__ == "__main__":
    main()
