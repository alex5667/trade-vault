from __future__ import annotations

"""ATR Policy Callback Ingress Watchdog — Phase 3.8 (Disaster Layer).

Watches for callback silence while Telegram proposals are pending.

Scenario A (Telegram callback stream dead):
  - proposals in pending queue
  - last_callback age > warn/critical thresholds
  - → warn/critical escalation, NO auto-revoke

Logic:
  - Publisher stamps atr_policy:telegram:last_notify_ts_ms after each push
  - Callback worker stamps atr_policy:telegram:last_callback_ts_ms after any valid callback
  - This watchdog reads both and compares against pending backlog

ENV:
  ATR_POLICY_CALLBACK_WATCHDOG_ENABLE     default 1
  ATR_POLICY_CALLBACK_WARN_SEC            default 1800   (30 min)
  ATR_POLICY_CALLBACK_CRITICAL_SEC        default 7200   (2 h)
  ATR_POLICY_CALLBACK_WATCHDOG_INTERVAL_SEC  default 60
  REDIS_URL
"""

import json
import logging
import os
import time
from typing import Any

import redis
from prometheus_client import Counter, Gauge
from core.redis_keys import RedisStreams as RS

logger = logging.getLogger(__name__)

STREAM_ESC = RS.ATR_POLICY_ESCALATIONS

KEY_LAST_NOTIFY = "atr_policy:telegram:last_notify_ts_ms"
KEY_LAST_CALLBACK = "atr_policy:telegram:last_callback_ts_ms"

# ── Prometheus ────────────────────────────────────────────────────────────────

c_watchdog_total = Counter(
    "atr_policy_callback_watchdog_total",
    "ATR policy callback watchdog checks",
    ["severity"],
)
g_callback_silence_sec = Gauge(
    "atr_policy_callback_silence_sec",
    "Seconds since last valid Telegram callback while proposals pending",
)
g_notify_silence_sec = Gauge(
    "atr_policy_notify_silence_sec",
    "Seconds since last Telegram notify",
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _rconn() -> redis.Redis:
    return redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        decode_responses=True,
    )


def _enable() -> bool:
    return os.getenv("ATR_POLICY_CALLBACK_WATCHDOG_ENABLE", "1") == "1"


def _warn_sec() -> int:
    try:
        return int(os.getenv("ATR_POLICY_CALLBACK_WARN_SEC", "1800") or 1800)
    except Exception:
        return 1800


def _critical_sec() -> int:
    try:
        return int(os.getenv("ATR_POLICY_CALLBACK_CRITICAL_SEC", "7200") or 7200)
    except Exception:
        return 7200


def _publish(r: redis.Redis, payload: dict[str, Any]) -> None:
    try:
        r.xadd(STREAM_ESC, {k: str(v) for k, v in payload.items()}, maxlen=2000)
    except Exception as exc:
        logger.warning("callback_watchdog: stream publish failed: %s", exc)


def _pending_ids(r: redis.Redis) -> list[str]:
    try:
        return list(r.smembers("queue:atr_policy:pending") or [])
    except Exception:
        return []


def _pending_submitted_count(r: redis.Redis, ids: list[str]) -> int:
    count = 0
    for pid in ids:
        raw = r.get(f"cfg:proposals:atr_policy:{pid}")
        if not raw:
            continue
        try:
            obj = json.loads(raw)
            if (obj.get("status", "")) == "SUBMITTED":
                count += 1
        except Exception:
            continue
    return count


# ── Core ──────────────────────────────────────────────────────────────────────

def check_once(r: redis.Redis | None = None) -> dict[str, Any]:
    """
    Run one watchdog check. Returns severity + metrics.

    severity: OK | WARN | CRITICAL
    """
    if not _enable():
        return {"severity": "OK", "reason": "WATCHDOG_DISABLED"}

    r = r or _rconn()
    now_ms = int(time.time() * 1000)
    now_sec = now_ms // 1000

    # ── Pending backlog ───────────────────────────────────────────────────
    pending_ids = _pending_ids(r)
    pending_submitted = _pending_submitted_count(r, pending_ids)

    if pending_submitted == 0:
        # No backlog — nothing to watch
        g_callback_silence_sec.set(0)
        c_watchdog_total.labels(severity="OK").inc()
        return {"severity": "OK", "reason": "NO_PENDING_BACKLOG", "pending_submitted": 0}

    # ── Callback age ──────────────────────────────────────────────────────
    last_callback_raw = r.get(KEY_LAST_CALLBACK)  # type: ignore
    last_notify_raw = r.get(KEY_LAST_NOTIFY)  # type: ignore
  # type: ignore
    last_callback_ms = int(last_callback_raw or 0)
    last_notify_ms = int(last_notify_raw or 0)

    callback_age_sec = max(0, now_sec - last_callback_ms // 1000) if last_callback_ms else 999_999
    notify_age_sec = max(0, now_sec - last_notify_ms // 1000) if last_notify_ms else 999_999

    g_callback_silence_sec.set(callback_age_sec)
    g_notify_silence_sec.set(notify_age_sec)

    # ── Severity classification ───────────────────────────────────────────
    severity = "OK"
    reason = "CALLBACK_RECENT"

    if callback_age_sec >= _critical_sec():
        severity = "CRITICAL"
        reason = "CALLBACK_SILENCE_CRITICAL"
    elif callback_age_sec >= _warn_sec():
        severity = "WARN"
        reason = "CALLBACK_SILENCE_WARN"

    c_watchdog_total.labels(severity=severity).inc()

    result: dict[str, Any] = {
        "severity": severity,
        "reason": reason,
        "pending_submitted": pending_submitted,
        "callback_age_sec": callback_age_sec,
        "notify_age_sec": notify_age_sec,
        "ts_ms": now_ms,
        "warn_threshold_sec": _warn_sec(),
        "critical_threshold_sec": _critical_sec(),
    }

    if severity in ("WARN", "CRITICAL"):
        _publish(r, {
            "event": f"TELEGRAM_CALLBACK_{severity}",
            **result,
        })
        logger.warning(
            "callback_watchdog: %s — pending=%d callback_age=%ds",
            severity, pending_submitted, callback_age_sec,
        )
    else:
        logger.debug(
            "callback_watchdog: OK — pending=%d callback_age=%ds",
            pending_submitted, callback_age_sec,
        )

    return result


def stamp_last_notify(r: redis.Redis | None = None) -> None:
    """Call from Telegram publisher after successful push."""
    r = r or _rconn()
    r.set(KEY_LAST_NOTIFY, int(time.time() * 1000))  # type: ignore
  # type: ignore

def stamp_last_callback(r: redis.Redis | None = None) -> None:
    """Call from Telegram callback_worker after any valid callback."""
    r = r or _rconn()
    r.set(KEY_LAST_CALLBACK, int(time.time() * 1000))  # type: ignore
  # type: ignore

def run_forever() -> None:
    """Watchdog loop. Usually run as a sidecar or integrated in of_timers_worker."""
    interval = int(os.getenv("ATR_POLICY_CALLBACK_WATCHDOG_INTERVAL_SEC", "60") or 60)
    logger.info("callback_watchdog: started, interval=%ds", interval)
    r = _rconn()
    while True:
        try:
            result = check_once(r)
            if result.get("severity") == "CRITICAL":
                logger.error(
                    "callback_watchdog: CRITICAL — pending=%s callback_age=%ss",
                    result.get("pending_submitted"),
                    result.get("callback_age_sec"),
                )
        except Exception as exc:
            logger.exception("callback_watchdog: check failed: %s", exc)
        time.sleep(interval)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_forever()
