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
5. When ``CV_CAL_AUTO_PROMOTE=1``: auto-flips ``enforce=True`` after all
   symbols reach ``min_samples`` AND ``promote_dwell_min`` minutes elapse,
   subject to sanity bounds on the computed thresholds.
   Workers pick up the new enforce=True snapshot within their next reader
   refresh cycle (≤ ``AUTOCAL_CROSSVENUE_REFRESH_MS``, default 60 s).
   No container restart needed.

Adaptive formulas (robust, no numpy):
  adaptive_disloc_z  = max(1.5, median(disloc) + 2.5 × MAD(disloc))
  adaptive_min_agree = clamp(median(agree) − 2.0 × MAD(agree), 0.50, 0.85)

ENV
---
  CV_CAL_REDIS_URL          Feed Redis URL (default REDIS_URL or redis://redis:6379/0)
  CV_CAL_PORT               Prometheus port (default 9920)
  CV_CAL_POLL_SEC           Polling interval in seconds (default 60)
  CV_CAL_SNAPSHOT_TTL       Redis TTL for autocal key in seconds (default 3600)
  CV_CAL_ENFORCE            0|1 — start in enforce mode (default 0)
  CV_CAL_WINDOW_MS          Rolling window in ms (default 86400000 = 24 h)
  CV_CAL_MIN_SAMPLES        Minimum samples before calibration active (default 30)
  CV_CAL_KEY_PREFIX         Redis key prefix to scan (default "ctx:crossvenue:")

Auto-promote (Step 3+4 — no container restart required):
  CV_CAL_AUTO_PROMOTE       0|1 — enable auto-promotion to enforce (default 0)
  CV_CAL_PROMOTE_DWELL_MIN  Minutes all symbols must be ready before promote (default 15)
  CV_CAL_PROMOTE_MAX_DISLOC_Z  Sanity cap on adaptive_disloc_z for promotion (default 5.0)
  CV_CAL_PROMOTE_MIN_AGREE     Sanity floor on adaptive_min_agree (default 0.55)
  CV_CAL_PROMOTE_MAX_AGREE     Sanity cap  on adaptive_min_agree (default 0.82)
  CV_CAL_NOTIFY_STREAM      Redis stream for Telegram notifications (default notify:telegram)

Rollback
--------
- DEL autocal:crossvenue:state  → readers return ENV defaults on next refresh (≤60 s).
- Or restart this container with CV_CAL_ENFORCE=0 → publishes enforce=false snapshot.
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
from core.redis_keys import RK, RS
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


_g_symbols_active     = _gauge("crossvenue_cal_symbols_active",       "Number of active crossvenue symbols scanned")
_g_symbols_calibrated = _gauge("crossvenue_cal_symbols_calibrated",   "Symbols with >= min_samples observations")
_g_adaptive_disloc    = _gauge("crossvenue_cal_adaptive_disloc_z",    "Per-symbol adaptive dislocation_z threshold", ["symbol"])
_g_adaptive_agree     = _gauge("crossvenue_cal_adaptive_min_agree",   "Per-symbol adaptive min_agree threshold",     ["symbol"])
_g_n_samples          = _gauge("crossvenue_cal_n_samples",            "Number of samples in rolling window",         ["symbol"])
_g_enforce            = _gauge("crossvenue_cal_enforce",              "1 if calibrator is in enforce mode")
_g_ready_age_sec      = _gauge("crossvenue_cal_ready_age_sec",        "Seconds since all symbols first reached min_samples")
_g_auto_promote       = _gauge("crossvenue_cal_auto_promote_enabled", "1 if auto-promote is configured")
_c_polls              = _counter("crossvenue_cal_polls_total",        "Total scan/poll cycles")
_c_obs                = _counter("crossvenue_cal_observations_total", "Total snapshot observations ingested")
_c_snap               = _counter("crossvenue_cal_snapshots_total",    "Total Redis snapshot publishes")
_c_promote            = _counter("crossvenue_cal_promote_total",      "Auto-promote attempts by result", ["result"])
_c_errors             = _counter("crossvenue_cal_errors_total",       "Errors by kind",                  ["kind"])
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
    """Scan ctx:crossvenue:* keys and observe current snapshots. Returns observations count."""
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
    """Update per-symbol gauges. Returns count of calibrated symbols."""
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
# Auto-promote helpers
# --------------------------------------------------------------------------

def _sanity_ok(
    calibrator: CrossVenueCalibratorCore,
    *,
    max_disloc: float,
    min_agree: float,
    max_agree: float,
) -> tuple[bool, str]:
    """Return (ok, reason) for all calibrated bins."""
    for sym, b in calibrator._bins.items():
        if len(b.buf) < calibrator.min_samples:
            continue
        if b.adaptive_disloc_z > max_disloc:
            return False, f"{sym}: disloc_z={b.adaptive_disloc_z:.3f} > cap {max_disloc}"
        if b.adaptive_min_agree < min_agree:
            return False, f"{sym}: min_agree={b.adaptive_min_agree:.3f} < floor {min_agree}"
        if b.adaptive_min_agree > max_agree:
            return False, f"{sym}: min_agree={b.adaptive_min_agree:.3f} > cap {max_agree}"
    return True, "ok"


def _send_telegram(
    r: Any,
    *,
    notify_stream: str,
    text: str,
) -> None:
    try:
        r.xadd(
            notify_stream,
            {
                "type": "report",
                "subtype": "crossvenue_cal_autopromote",
                "ts": str(int(time.time() * 1000)),
                "text": text,
                "parse_mode": "HTML",
            },
            maxlen=50_000,
        )
    except Exception as e:
        logger.warning("Telegram notify failed: %s", e)


def _do_promote(
    calibrator: CrossVenueCalibratorCore,
    r: Any,
    *,
    snapshot_ttl: int,
    notify_stream: str,
    n_active: int,
    n_calibrated: int,
    dwell_min: int,
) -> None:
    """Flip enforce=True, publish snapshot, notify Telegram."""
    calibrator.enforce = True
    now_ms = int(time.time() * 1000)
    snap = calibrator.snapshot(now_ms)
    r.set(RK.AUTOCAL_CROSSVENUE_STATE, json.dumps(snap, separators=(",", ":")), ex=snapshot_ttl)

    summary_lines = []
    for sym, b in calibrator._bins.items():
        if len(b.buf) >= calibrator.min_samples:
            summary_lines.append(
                f"  • <b>{sym}</b>: disloc_z={b.adaptive_disloc_z:.2f}  min_agree={b.adaptive_min_agree:.2f}"
            )

    text = (
        f"<b>✅ CrossVenue Calibrator — enforce АКТИВИРОВАН</b>\n\n"
        f"Все <b>{n_calibrated}/{n_active}</b> символов достигли "
        f"≥{calibrator.min_samples} сэмплов (dwell {dwell_min} мин).\n\n"
        f"Адаптивные пороги:\n" + "\n".join(summary_lines) + "\n\n"
        f"Workers подхватят enforce=true в течение ≤60 с.\n"
        f"Rollback: <code>docker exec redis redis-cli DEL {RK.AUTOCAL_CROSSVENUE_STATE}</code>"
    )
    _send_telegram(r, notify_stream=notify_stream, text=text)
    logger.info(
        "AUTO-PROMOTE: enforce=True published | n_symbols=%d dwell_min=%d",
        n_calibrated, dwell_min,
    )
    _c_promote.labels(result="success").inc()


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------

def main() -> None:  # pragma: no cover — integration entrypoint
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    port            = _env_int("CV_CAL_PORT",              9920)
    poll_sec        = _env_int("CV_CAL_POLL_SEC",          60)
    snapshot_ttl    = _env_int("CV_CAL_SNAPSHOT_TTL",      3600)
    enforce         = _env_bool("CV_CAL_ENFORCE",          False)
    window_ms       = _env_int("CV_CAL_WINDOW_MS",         86_400_000)
    min_samples     = _env_int("CV_CAL_MIN_SAMPLES",       30)
    key_prefix      = _env("CV_CAL_KEY_PREFIX",            "ctx:crossvenue:")

    auto_promote    = _env_bool("CV_CAL_AUTO_PROMOTE",     False)
    dwell_min       = _env_int("CV_CAL_PROMOTE_DWELL_MIN", 15)
    max_disloc      = _env_float("CV_CAL_PROMOTE_MAX_DISLOC_Z", 5.0)
    min_agree_floor = _env_float("CV_CAL_PROMOTE_MIN_AGREE",    0.55)
    min_agree_cap   = _env_float("CV_CAL_PROMOTE_MAX_AGREE",    0.82)
    notify_stream   = _env("CV_CAL_NOTIFY_STREAM",         RS.NOTIFY_TELEGRAM)

    redis_url = _env("CV_CAL_REDIS_URL", _env("REDIS_URL", "redis://redis:6379/0"))
    logger.info(
        "cross_venue_calibrator_v1 starting | port=%d poll_sec=%d enforce=%s "
        "auto_promote=%s dwell_min=%d window_h=%.1f min_samples=%d redis=%s",
        port, poll_sec, enforce, auto_promote, dwell_min,
        window_ms / 3_600_000, min_samples, redis_url,
    )

    start_http_server(port)
    _g_auto_promote.set(1.0 if auto_promote else 0.0)
    _g_ready_age_sec.set(0.0)

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
            for sym, b in prev._bins.items():
                calibrator._bins[sym] = b
            # Honour persisted enforce flag (previous promote survived restart)
            if prev.enforce and not enforce:
                calibrator.enforce = prev.enforce
                logger.info("Restored enforce=True from previous snapshot")
            logger.info("Restored %d committed thresholds from previous snapshot", len(calibrator._bins))
    except Exception as e:
        logger.warning("Could not restore previous snapshot: %s", e)

    _stop = False
    _ready_since = 0.0  # wall-clock time when all symbols first reached min_samples

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
        n_active = len(calibrator._bins)

        # ── Auto-promote (Steps 3+4): flip enforce=True via Redis snapshot ──
        if auto_promote and not calibrator.enforce and n_active > 0:
            if calibrated >= n_active:
                if _ready_since == 0.0:
                    _ready_since = time.time()
                    logger.info(
                        "All %d/%d symbols calibrated — dwell timer starts (%d min)",
                        calibrated, n_active, dwell_min,
                    )
                ready_age_sec = time.time() - _ready_since
                _g_ready_age_sec.set(ready_age_sec)

                if ready_age_sec >= dwell_min * 60:
                    ok, reason = _sanity_ok(
                        calibrator,
                        max_disloc=max_disloc,
                        min_agree=min_agree_floor,
                        max_agree=min_agree_cap,
                    )
                    if ok:
                        _do_promote(
                            calibrator, r,
                            snapshot_ttl=snapshot_ttl,
                            notify_stream=notify_stream,
                            n_active=n_active,
                            n_calibrated=calibrated,
                            dwell_min=dwell_min,
                        )
                    else:
                        logger.warning("Sanity check blocked promote: %s", reason)
                        _c_promote.labels(result="blocked_sanity").inc()
                else:
                    _c_promote.labels(result="dwell_pending").inc()
                    logger.debug(
                        "Dwell pending: %.1f / %d min",
                        ready_age_sec / 60, dwell_min,
                    )
            else:
                if _ready_since > 0.0:
                    logger.info("Calibrated count dropped (%d/%d) — resetting dwell timer", calibrated, n_active)
                _ready_since = 0.0
                _g_ready_age_sec.set(0.0)

        # Publish snapshot (always — even in shadow mode, readers see thresholds)
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
            "poll done | obs=%d updated=%d calibrated=%d/%d enforce=%s elapsed_ms=%.1f",
            obs, updated, calibrated, n_active, calibrator.enforce, elapsed_ms,
        )

    logger.info("cross_venue_calibrator_v1 stopped")


if __name__ == "__main__":
    main()
