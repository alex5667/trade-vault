"""
preflight_reader_flip_service.py — periodic wrapper around
tools.preflight_reader_flip, exposes Prometheus metrics.

For each reader (adaptive_ttl, ensemble) every PREFLIGHT_INTERVAL_SEC:
  * runs the same 4-check go/no-go suite as the CLI tool
  * sets gauge preflight_reader_check_passed{reader} = 0/1
  * increments counter preflight_reader_check_total{reader,status}
  * increments counter preflight_reader_check_failure_total{reader,check}
    for any failing check (helps alert on the exact reason)
  * logs one structured line per cycle

Operator workflow:
  Alert on "preflight_reader_check_passed{reader=X} == 1 for 24h" →
  safe to flip ADAPTIVE_TTL_READ_ENABLED=1 / ENSEMBLE_WEIGHTS_READ_ENABLED=1.

ENV:
  PREFLIGHT_INTERVAL_SEC      = 900    (15 min, mirrors publisher cadence)
  PREFLIGHT_PORT              = 9960
  PREFLIGHT_READERS           = adaptive_ttl,ensemble
  REDIS_URL                   = redis://redis-worker-1:6379/0
  PREFLIGHT_MAX_AGE_MIN       = 120    (passed through to checker)
  PREFLIGHT_MIN_RECS          = 1
  PREFLIGHT_MIN_SYMBOLS       = 1
  PREFLIGHT_MIN_SOURCES       = 2
  METRICS_HOST                = adaptive-ttl-publisher / ensemble-weights-publisher
                                (or comma list: a,b) — for Prometheus probes
"""
from __future__ import annotations

import logging
import os
import time

log = logging.getLogger("preflight_reader_flip_service")


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


def main() -> None:
    from prometheus_client import Counter, Gauge, start_http_server

    from tools.preflight_reader_flip import (
        check_adaptive_ttl,
        check_ensemble_weights,
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    interval_sec = _env_int("PREFLIGHT_INTERVAL_SEC", 900)
    port         = _env_int("PREFLIGHT_PORT", 9960)
    readers_csv  = _env("PREFLIGHT_READERS", "adaptive_ttl,ensemble")
    readers      = [r.strip() for r in readers_csv.split(",") if r.strip()]

    start_http_server(port)

    g_passed   = Gauge("preflight_reader_check_passed",
                       "1 if reader preflight passed, 0 otherwise", ["reader"])
    c_total    = Counter("preflight_reader_check_total",
                         "Preflight cycles", ["reader", "status"])
    c_fail     = Counter("preflight_reader_check_failure_total",
                         "Individual check failures", ["reader", "check"])
    g_age_sec  = Gauge("preflight_reader_last_check_age_sec",
                       "Seconds since last check (per reader)", ["reader"])
    g_last_ms  = Gauge("preflight_reader_last_check_ts_ms",
                       "Wallclock of last check (per reader)", ["reader"])

    # Initialise gauges so Prometheus sees them immediately
    for r in readers:
        g_passed.labels(reader=r).set(0)

    log.info(
        "preflight_reader_flip_service starting | port=%d interval=%ds readers=%s",
        port, interval_sec, readers,
    )

    last_check_at: dict[str, float] = {r: 0.0 for r in readers}

    checkers = {
        "adaptive_ttl": check_adaptive_ttl,
        "ensemble":     check_ensemble_weights,
    }

    while True:
        cycle_start = time.monotonic()

        for reader in readers:
            fn = checkers.get(reader)
            if fn is None:
                log.warning("preflight: unknown reader=%s, skipping", reader)
                continue
            try:
                report = fn()
            except Exception as e:
                log.warning("preflight reader=%s exception: %s", reader, e)
                c_total.labels(reader=reader, status="error").inc()
                g_passed.labels(reader=reader).set(0)
                continue

            now = time.time()
            last_check_at[reader] = now
            g_last_ms.labels(reader=reader).set(int(now * 1000))

            if report.passed:
                g_passed.labels(reader=reader).set(1)
                c_total.labels(reader=reader, status="pass").inc()
                log.info(
                    "preflight reader=%s PASS checks=%s",
                    reader, [c.name for c in report.checks],
                )
            else:
                g_passed.labels(reader=reader).set(0)
                c_total.labels(reader=reader, status="fail").inc()
                failed = [c for c in report.checks if not c.passed]
                for c in failed:
                    c_fail.labels(reader=reader, check=c.name).inc()
                log.warning(
                    "preflight reader=%s FAIL failed=%s",
                    reader,
                    [(c.name, c.detail) for c in failed],
                )

        # update freshness gauge for all readers
        now_mono = time.monotonic()
        for r, last in last_check_at.items():
            if last > 0:
                g_age_sec.labels(reader=r).set(time.time() - last)

        # sleep until next interval
        elapsed = time.monotonic() - cycle_start
        sleep_for = max(1.0, interval_sec - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
