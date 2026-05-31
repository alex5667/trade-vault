#!/usr/bin/env python3
"""macro_calendar_scheduler.py — High-impact macro event proximity signals.

Reads a statically-configured list of scheduled macro events (FOMC, CPI, NFP,
PCE, PPI) and publishes real-time proximity metrics to Redis every minute.

Redis output (STRING / JSON):
  ctx:macro:global  — TTL 3600s

Payload fields:
  ts_ms                    epoch_ms (now)
  macro_event_severity     0=none, 1=medium, 2=high (of the nearest active event)
  minutes_to_macro_event   minutes until next future event (capped at 10080 = 1 week)
  minutes_after_macro_event minutes since last event (capped at 10080; 0 if in future)
  event_name               name of the controlling event ("CPI", "FOMC", etc.)
  quality_status           "OK" or "STALE"

Severity tiers:
  HIGH  (2): FOMC rate decision, US CPI, NFP, PCE Core
  MEDIUM (1): PPI, JOLTS, Retail Sales, Fed Chair speech
  NONE  (0): no event within MACRO_ACTIVE_WINDOW_MIN of now

Active window: ±MACRO_ACTIVE_WINDOW_MIN around event time.
  Within window: macro_event_severity = event tier, minutes_to = 0 (or negative →
  clamped to 0), minutes_after = elapsed since start.

ENV:
  MACRO_INTERVAL_S           poll interval (default 60)
  MACRO_ACTIVE_WINDOW_MIN    window on each side of event (default 120)
  MACRO_TTL_S                Redis TTL (default 3600)
  CTX_MAIN_REDIS_URL         Redis URL (default redis://redis:6379/0)
  MACRO_CALENDAR_PATH        path to optional JSON calendar override
"""
from __future__ import annotations

import json
import logging
import os
import signal
import time
from datetime import datetime
from typing import Any

logger = logging.getLogger("macro_calendar")

_INTERVAL_S: int = int(os.getenv("MACRO_INTERVAL_S", "60"))
_ACTIVE_WIN_MIN: float = float(os.getenv("MACRO_ACTIVE_WINDOW_MIN", "120"))
_TTL_S: int = int(os.getenv("MACRO_TTL_S", "3600"))
_MAX_HORIZON_MIN: float = 10_080.0  # 1 week cap

# ---------------------------------------------------------------------------
# Built-in 2026 calendar (UTC times; FOMC = statement day 14:00 ET = 19:00 UTC)
# CPI/NFP/PCE = BLS/BEA release times (08:30 ET = 13:30 UTC)
# Update this list when BLS/Fed publishes next year's schedule.
# ---------------------------------------------------------------------------

HIGH = 2
MEDIUM = 1

_BUILTIN_EVENTS: list[dict[str, Any]] = [
    # ── FOMC rate decisions 2026 (Fed publishes: 14:00 ET → 19:00 UTC) ──
    {"name": "FOMC", "utc": "2026-01-29T19:00:00Z", "severity": HIGH},
    {"name": "FOMC", "utc": "2026-03-18T18:00:00Z", "severity": HIGH},
    {"name": "FOMC", "utc": "2026-05-06T18:00:00Z", "severity": HIGH},
    {"name": "FOMC", "utc": "2026-06-17T18:00:00Z", "severity": HIGH},
    {"name": "FOMC", "utc": "2026-07-29T18:00:00Z", "severity": HIGH},
    {"name": "FOMC", "utc": "2026-09-16T18:00:00Z", "severity": HIGH},
    {"name": "FOMC", "utc": "2026-11-04T19:00:00Z", "severity": HIGH},
    {"name": "FOMC", "utc": "2026-12-16T19:00:00Z", "severity": HIGH},
    # ── US CPI (BLS, 08:30 ET → 13:30 UTC, ~2nd week of month) ──
    {"name": "CPI",  "utc": "2026-01-15T13:30:00Z", "severity": HIGH},
    {"name": "CPI",  "utc": "2026-02-12T13:30:00Z", "severity": HIGH},
    {"name": "CPI",  "utc": "2026-03-12T13:30:00Z", "severity": HIGH},
    {"name": "CPI",  "utc": "2026-04-10T12:30:00Z", "severity": HIGH},
    {"name": "CPI",  "utc": "2026-05-13T12:30:00Z", "severity": HIGH},
    {"name": "CPI",  "utc": "2026-06-11T12:30:00Z", "severity": HIGH},
    {"name": "CPI",  "utc": "2026-07-15T12:30:00Z", "severity": HIGH},
    {"name": "CPI",  "utc": "2026-08-13T12:30:00Z", "severity": HIGH},
    {"name": "CPI",  "utc": "2026-09-10T12:30:00Z", "severity": HIGH},
    {"name": "CPI",  "utc": "2026-10-14T12:30:00Z", "severity": HIGH},
    {"name": "CPI",  "utc": "2026-11-12T13:30:00Z", "severity": HIGH},
    {"name": "CPI",  "utc": "2026-12-10T13:30:00Z", "severity": HIGH},
    # ── NFP (BLS, first Friday 08:30 ET → 13:30 UTC) ──
    {"name": "NFP",  "utc": "2026-01-09T13:30:00Z", "severity": HIGH},
    {"name": "NFP",  "utc": "2026-02-06T13:30:00Z", "severity": HIGH},
    {"name": "NFP",  "utc": "2026-03-06T13:30:00Z", "severity": HIGH},
    {"name": "NFP",  "utc": "2026-04-03T12:30:00Z", "severity": HIGH},
    {"name": "NFP",  "utc": "2026-05-08T12:30:00Z", "severity": HIGH},
    {"name": "NFP",  "utc": "2026-06-05T12:30:00Z", "severity": HIGH},
    {"name": "NFP",  "utc": "2026-07-10T12:30:00Z", "severity": HIGH},
    {"name": "NFP",  "utc": "2026-08-07T12:30:00Z", "severity": HIGH},
    {"name": "NFP",  "utc": "2026-09-04T12:30:00Z", "severity": HIGH},
    {"name": "NFP",  "utc": "2026-10-09T12:30:00Z", "severity": HIGH},
    {"name": "NFP",  "utc": "2026-11-06T13:30:00Z", "severity": HIGH},
    {"name": "NFP",  "utc": "2026-12-04T13:30:00Z", "severity": HIGH},
    # ── Core PCE (BEA, end of month 08:30 ET → 13:30 UTC) ──
    {"name": "PCE",  "utc": "2026-01-30T13:30:00Z", "severity": HIGH},
    {"name": "PCE",  "utc": "2026-02-27T13:30:00Z", "severity": HIGH},
    {"name": "PCE",  "utc": "2026-03-27T12:30:00Z", "severity": HIGH},
    {"name": "PCE",  "utc": "2026-04-30T12:30:00Z", "severity": HIGH},
    {"name": "PCE",  "utc": "2026-05-29T12:30:00Z", "severity": HIGH},
    {"name": "PCE",  "utc": "2026-06-26T12:30:00Z", "severity": HIGH},
    {"name": "PCE",  "utc": "2026-07-31T12:30:00Z", "severity": HIGH},
    {"name": "PCE",  "utc": "2026-08-28T12:30:00Z", "severity": HIGH},
    {"name": "PCE",  "utc": "2026-09-25T12:30:00Z", "severity": HIGH},
    {"name": "PCE",  "utc": "2026-10-30T12:30:00Z", "severity": HIGH},
    {"name": "PCE",  "utc": "2026-11-25T13:30:00Z", "severity": HIGH},
    {"name": "PCE",  "utc": "2026-12-18T13:30:00Z", "severity": HIGH},
    # ── PPI (BLS, day before CPI, 08:30 ET) ──
    {"name": "PPI",  "utc": "2026-01-14T13:30:00Z", "severity": MEDIUM},
    {"name": "PPI",  "utc": "2026-02-11T13:30:00Z", "severity": MEDIUM},
    {"name": "PPI",  "utc": "2026-03-11T13:30:00Z", "severity": MEDIUM},
    {"name": "PPI",  "utc": "2026-04-09T12:30:00Z", "severity": MEDIUM},
    {"name": "PPI",  "utc": "2026-05-12T12:30:00Z", "severity": MEDIUM},
    {"name": "PPI",  "utc": "2026-06-10T12:30:00Z", "severity": MEDIUM},
    {"name": "PPI",  "utc": "2026-07-14T12:30:00Z", "severity": MEDIUM},
    {"name": "PPI",  "utc": "2026-08-12T12:30:00Z", "severity": MEDIUM},
    {"name": "PPI",  "utc": "2026-09-09T12:30:00Z", "severity": MEDIUM},
    {"name": "PPI",  "utc": "2026-10-13T12:30:00Z", "severity": MEDIUM},
    {"name": "PPI",  "utc": "2026-11-11T13:30:00Z", "severity": MEDIUM},
    {"name": "PPI",  "utc": "2026-12-09T13:30:00Z", "severity": MEDIUM},
]


def _load_events(path: str | None = None) -> list[dict[str, Any]]:
    if path and os.path.isfile(path):
        try:
            with open(path) as f:
                data = json.load(f)
            evts = data if isinstance(data, list) else data.get("events", [])
            logger.info("Loaded %d events from %s", len(evts), path)
            return evts
        except Exception as e:
            logger.warning("Failed to load calendar override %s: %s — using built-in", path, e)
    return list(_BUILTIN_EVENTS)


def _parse_events(raw: list[dict[str, Any]]) -> list[tuple[float, str, int]]:
    """Return sorted list of (ts_ms, name, severity)."""
    parsed: list[tuple[float, str, int]] = []
    for ev in raw:
        utc = ev.get("utc") or ev.get("datetime") or ""
        name = str(ev.get("name") or "EVENT")
        severity = int(ev.get("severity") or HIGH)
        if not utc:
            continue
        try:
            dt = datetime.fromisoformat(utc.replace("Z", "+00:00"))
            ts_ms = dt.timestamp() * 1000.0
            parsed.append((ts_ms, name, severity))
        except Exception:
            logger.debug("Cannot parse event date: %s", utc)
    parsed.sort(key=lambda x: x[0])
    return parsed


def compute_proximity(
    events: list[tuple[float, str, int]],
    now_ms: float,
    active_win_min: float,
) -> dict[str, Any]:
    """Compute macro proximity metrics from sorted event list."""
    active_win_ms = active_win_min * 60_000.0

    best_severity = 0
    best_name = "none"
    min_to_future: float = _MAX_HORIZON_MIN
    max_after_past: float = 0.0
    controlling_name = "none"

    for ts_ms, name, severity in events:
        delta_ms = ts_ms - now_ms  # positive = future
        delta_min = delta_ms / 60_000.0

        if abs(delta_ms) <= active_win_ms:
            # within the active window: this event is "live"
            if severity > best_severity:
                best_severity = severity
                best_name = name

        if delta_min > 0:
            # future event
            if delta_min < min_to_future:
                min_to_future = delta_min
                if best_severity == 0:
                    controlling_name = name
        else:
            # past event
            elapsed = -delta_min
            if elapsed < _MAX_HORIZON_MIN and elapsed > max_after_past:
                max_after_past = elapsed
                if best_severity == 0:
                    controlling_name = name

    min_to = min(min_to_future, _MAX_HORIZON_MIN)
    min_after = min(max_after_past, _MAX_HORIZON_MIN)

    if best_severity > 0:
        controlling_name = best_name
        # if inside active window, minutes_to → 0
        min_to = 0.0

    return {
        "macro_event_severity": float(best_severity),
        "minutes_to_macro_event": round(min_to, 1),
        "minutes_after_macro_event": round(min_after, 1),
        "event_name": controlling_name,
    }


def publish(redis_client: Any, payload: dict[str, Any], ttl_s: int) -> None:
    key = "ctx:macro:global"
    try:
        redis_client.set(key, json.dumps(payload), ex=ttl_s)
    except Exception as e:
        logger.warning("Redis SET %s failed: %s", key, e)


def run_once(redis_client: Any, events: list[tuple[float, str, int]]) -> dict[str, Any]:
    now_ms = time.time() * 1000.0
    prox = compute_proximity(events, now_ms, _ACTIVE_WIN_MIN)
    payload: dict[str, Any] = {
        "schema_version": 1,
        "ts_ms": int(now_ms),
        "quality_status": "OK",
        **prox,
    }
    publish(redis_client, payload, _TTL_S)
    return payload


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    cal_path = os.getenv("MACRO_CALENDAR_PATH")
    raw_events = _load_events(cal_path)
    events = _parse_events(raw_events)
    logger.info("Macro calendar: %d events loaded (active_win=±%.0fmin)", len(events), _ACTIVE_WIN_MIN)

    import redis as _redis
    redis_url = os.getenv("CTX_MAIN_REDIS_URL", "redis://redis:6379/0")
    redis_client = _redis.from_url(redis_url, socket_timeout=3.0, socket_connect_timeout=3.0)

    stop = {"flag": False}

    def _sig(_a: int, _b: Any) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    while not stop["flag"]:
        try:
            p = run_once(redis_client, events)
            logger.debug(
                "macro: severity=%d min_to=%.1f min_after=%.1f event=%s",
                int(p["macro_event_severity"]),
                p["minutes_to_macro_event"],
                p["minutes_after_macro_event"],
                p["event_name"],
            )
        except Exception as e:
            logger.error("run_once failed: %s", e)
        for _ in range(_INTERVAL_S):
            if stop["flag"]:
                break
            time.sleep(1)

    logger.info("macro_calendar_scheduler stopped")


if __name__ == "__main__":
    main()
