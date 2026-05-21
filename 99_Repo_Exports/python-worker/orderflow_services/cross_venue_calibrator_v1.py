#!/usr/bin/env python3
"""cross_venue_calibrator_v1.py

Feed-side service for the cross-venue gate autocalibrator.

What it does
------------
1. Every ``poll_sec`` (default 60 s) scans Redis for all live
   ``ctx:crossvenue:{SYMBOL}`` keys written by the Go CrossVenueAggregator.
2. Calls ``CrossVenueCalibratorCore.observe(symbol, disloc_z, agree, ts_ms)``
   for every fresh snapshot found.
3. Recomputes adaptive thresholds per symbol after each poll.
4. Publishes ``CrossVenueCalibratorCore.snapshot()`` to
   ``autocal:crossvenue:state`` (Redis SET with TTL).

Adaptive formulas (robust, no numpy):
  adaptive_disloc_z  = max(1.5, median(disloc) + 2.5 × MAD(disloc))
  adaptive_min_agree = clamp(median(agree) − 2.0 × MAD(agree), 0.50, 0.85)

ENV
---
  CV_CAL_REDIS_URL      Feed Redis URL (default REDIS_URL or redis://redis:6379/0)
  CV_CAL_PORT           Prometheus port (default 9920)
  CV_CAL_POLL_SEC       Polling interval in seconds (default 60)
  CV_CAL_SNAPSHOT_TTL   Redis TTL for autocal key in seconds (default 3600)
  CV_CAL_ENFORCE        0|1 — promote calibrated thresholds to enforce (default 0)
  CV_CAL_WINDOW_MS      Rolling window in ms (default 86400000 = 24 h)
  CV_CAL_MIN_SAMPLES    Minimum samples before calibration active (default 30)
  CV_CAL_KEY_PREFIX     Redis key prefix to scan (default "ctx:crossvenue:")

Rollout
-------
1. Deploy service (ENFORCE=0 by default → shadow only, metrics only).
2. After ≥ 30 min with ≥ 30 samples per active symbol:
   - Check ``crossvenue_cal_symbols_calibrated`` == active symbol count.
   - Set ``AUTOCAL_CROSSVENUE_READ_ENABLED=1`` in signal_pipeline containers.
   - Set ``CV_CAL_ENFORCE=1`` here to publish enforce=true.
3. Monitor ``crossvenue_cal_adaptive_disloc_z`` / ``crossvenue_cal_adaptive_min_agree``
   gauges for drift.

Rollback
--------
- Set ``AUTOCAL_CROSSVENUE_READ_ENABLED=0`` on signal_pipeline containers.
- Reader immediately falls back to ENV defaults (fail-open).
"""
from __future__ import annotations

import json
import logging
import os
import signal
import time
from typing import Any

from prometheus_client import Counter, Gauge, Histogram, REGISTRY, start_http_server  # type: ignore

from core.cross_venue_calibrator import CrossVenueCalibratorCore
from core.redis_client import get_redis
from core.redis_keys import RK
from services.orderflow.crossvenue_context import from_dict as _cv_from_dict

logger = logging.getLogger("cv-cal")


# --------------------------------------------------------------------------
# env helpers
# --------------------------------------------------------------------------

def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default) or default


def _env_int(name: str, default: int) -> int:
    raw = _env(name, "")
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = _env(name, "")
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name, "")
    if not raw:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


# --------------------------------------------------------------------------
# Prometheus (idempotent registration helpers)
# --------------------------------------------------------------------------

def _gauge(name: str, doc: str, labels: list[str] | None = None) -> Gauge:
    try:
        return Gauge(name, doc, labels or [])
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore[return-value]
        raise


def _counter(name: str, doc: str, labels: list[str] | None = None) -> Counter:
    try:
        return Counter(name, doc, labels or [])
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore[return-value]
        raise


def _histogram(name: str, doc: str, buckets: list[float], labels: list[str] | None = None) -> Histogram:
    try:
        return Histogram(name, doc, labels or [], buckets=buckets)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore[return-value]
        raise


_g_symbols_active     = _gauge("crossvenue_cal_symbols_active",     "Number of active crossvenue symbols scanned")
_g_symbols_calibrated = _gauge("crossvenue_cal_symbols_calibrated", "Symbols with >= min_samples observations")
_g_adaptive_disloc    = _gauge("crossvenue_cal_adaptive_disloc_z",  "Per-symbol adaptive dislocation_z threshold", ["symbol"])
_g_adaptive_agree     = _gauge("crossvenue_cal_adaptive_min_agree", "Per-symbol adaptive min_agree threshold",     ["symbol"])
_g_n_samples          = _gauge("crossvenue_cal_n_samples",          "Number of samples in rolling window",         ["symbol"])
_g_enforce            = _gauge("crossvenue_cal_enforce",            "1 if calibrator is in enforce mode")
_c_polls              = _counter("crossvenue_cal_polls_total",      "Total scan/poll cycles")
_c_obs                = _counter("crossvenue_cal_observations_total", "Total snapshot observations ingested")
_c_snap               = _counter("crossvenue_cal_snapshots_total",  "Total Redis snapshot publishes")
_c_errors             = _counter("crossvenue_cal_errors_total",     "Errors by kind", ["kind"])
_h_poll_ms            = _histogram(
    "crossvenue_cal_poll_duration_ms",
    "Duration of one poll cycle in ms",
    [5, 10, 20, 50, 100, 250, 500, 1000],
)


# --------------------------------------------------------------------------
# Snapshot scan
# --------------------------------------------------------------------------

def _scan_crossvenue_keys(r: Any, prefix: str) -> list[str]:
    """Return all live ``{prefix}{SYMBOL}`` keys via SCAN."""
    keys: list[str] = []
    cursor = 0
    while True:
        cursor, batch = r.scan(cursor, match=f"{prefix}*", count=200)
        keys.extend(batch)
        if cursor == 0:
            break
    return keys


def _poll_once(
    r: Any,
    calibrator: CrossVenueCalibratorCore,
    *,
    prefix: str,
) -> int:
    """Scan ctx:crossvenue:* keys and observe current snapshots.  Returns observations count."""
    keys = _scan_crossvenue_keys(r, prefix)
    _g_symbols_active.set(len(keys))

    now_ms = int(time.time() * 1000)
    obs = 0
    for key in keys:
        try:
            raw = r.get(key)
            if raw is None:
                continue
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", "ignore")
            payload = json.loads(raw)
            cv = _cv_from_dict(payload)
            if cv is None:
                continue
            calibrator.observe(
                symbol=cv.symbol,
                disloc_z=cv.venue_dislocation_z,
                agree=cv.cross_venue_direction_agree,
                ts_ms=cv.ts_ms or now_ms,
            )
            obs += 1
        except Exception as e:
            logger.debug("poll_once: skipping key %s: %s", key, e)
            _c_errors.labels(kind="observe").inc()
    return obs


# --------------------------------------------------------------------------
# Prometheus metric update
# --------------------------------------------------------------------------

def _update_metrics(calibrator: CrossVenueCalibratorCore) -> int:
    """Update per-symbol gauges.  Returns count of calibrated symbols."""
    calibrated = 0
    for sym, b in calibrator._bins.items():
        n = len(b.buf)
        _g_n_samples.labels(symbol=sym).set(n)
        _g_adaptive_disloc.labels(symbol=sym).set(b.adaptive_disloc_z)
        _g_adaptive_agree.labels(symbol=sym).set(b.adaptive_min_agree)
        if n >= calibrator.min_samples:
            calibrated += 1
    _g_symbols_calibrated.set(calibrated)
    _g_enforce.set(1.0 if calibrator.enforce else 0.0)
    return calibrated


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------

def main() -> None:  # pragma: no cover — integration entrypoint
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    port         = _env_int("CV_CAL_PORT",         9920)
    poll_sec     = _env_int("CV_CAL_POLL_SEC",     60)
    snapshot_ttl = _env_int("CV_CAL_SNAPSHOT_TTL", 3600)
    enforce      = _env_bool("CV_CAL_ENFORCE",     False)
    window_ms    = _env_int("CV_CAL_WINDOW_MS",    86_400_000)
    min_samples  = _env_int("CV_CAL_MIN_SAMPLES",  30)
    key_prefix   = _env("CV_CAL_KEY_PREFIX",       "ctx:crossvenue:")

    redis_url = _env("CV_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis:6379/0"))
    logger.info(
        "cross_venue_calibrator_v1 starting | port=%d poll_sec=%d enforce=%s "
        "window_h=%.1f min_samples=%d redis=%s",
        port, poll_sec, enforce, window_ms / 3_600_000, min_samples, redis_url,
    )

    start_http_server(port)

    r = get_redis()

    # Restore committed thresholds from previous run (buffers empty → rebuilt live)
    calibrator = CrossVenueCalibratorCore(
        window_ms=window_ms,
        min_samples=min_samples,
        enforce=enforce,
    )
    try:
        raw_snap = r.get(RK.AUTOCAL_CROSSVENUE_STATE)
        if raw_snap:
            if isinstance(raw_snap, (bytes, bytearray)):
                raw_snap = raw_snap.decode("utf-8", "ignore")
            state = json.loads(raw_snap)
            prev = CrossVenueCalibratorCore.load_state(state)
            # Copy committed thresholds but honour current enforce flag
            for sym, b in prev._bins.items():
                calibrator._bins[sym] = b
            logger.info("Restored %d committed thresholds from previous snapshot", len(calibrator._bins))
    except Exception as e:
        logger.warning("Could not restore previous snapshot: %s", e)

    _stop = False

    def _handle_signal(sig: int, _: Any) -> None:
        nonlocal _stop
        logger.info("Received signal %d — stopping", sig)
        _stop = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    next_poll = time.time()

    while not _stop:
        now = time.time()
        if now < next_poll:
            time.sleep(min(0.5, next_poll - now))
            continue

        next_poll = now + poll_sec
        t0 = time.time()
        _c_polls.inc()

        try:
            obs = _poll_once(r, calibrator, prefix=key_prefix)
            _c_obs.inc(obs)
        except Exception as e:
            logger.error("poll_once failed: %s", e)
            _c_errors.labels(kind="poll").inc()
            continue

        now_ms = int(time.time() * 1000)
        updated = calibrator.recompute_all(now_ms)
        calibrated = _update_metrics(calibrator)

        # Publish snapshot
        try:
            snap = calibrator.snapshot(now_ms)
            r.set(RK.AUTOCAL_CROSSVENUE_STATE, json.dumps(snap, separators=(",", ":")), ex=snapshot_ttl)
            _c_snap.inc()
        except Exception as e:
            logger.error("snapshot publish failed: %s", e)
            _c_errors.labels(kind="snapshot").inc()

        elapsed_ms = (time.time() - t0) * 1000
        _h_poll_ms.observe(elapsed_ms)
        logger.info(
            "poll done | obs=%d updated=%d calibrated=%d elapsed_ms=%.1f",
            obs, updated, calibrated, elapsed_ms,
        )

    logger.info("cross_venue_calibrator_v1 stopped")


if __name__ == "__main__":
    main()
