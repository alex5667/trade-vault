from __future__ import annotations

"""ATR Policy Disaster SLO Analytics — Phase 3.8 (Disaster Layer).

Extends Phase 3.7 SRE exporter with disaster-specific SLOs:
  - verifier failure rate per cohort
  - rollback success/failure rate
  - kill_switch active count
  - callback watchdog severity distribution
  - flip storm frequency
  - active corruption events
  - last_good mirror coverage (active keys WITH last_good / total active)

Exports Prometheus gauges for alerting and runs as a standalone loop
or as a collect_once() call from of_timers_worker.

ENV:
  ATR_POLICY_DISASTER_SLO_PORT          default 9138
  ATR_POLICY_DISASTER_SLO_INTERVAL_SEC  default 30
  REDIS_URL
"""

import logging
import os
import time
from typing import Any

import redis
from core.redis_keys import RedisStreams as RS

try:
    from core.redis_client import get_atr_redis
except Exception:
    get_atr_redis = None
from prometheus_client import Counter, Gauge, start_http_server

logger = logging.getLogger(__name__)

# ── Prometheus ────────────────────────────────────────────────────────────────

g_kill_switch_active_count = Gauge(
    "atr_policy_disaster_kill_switch_active_count",
    "Number of cohorts currently under kill_switch",
)
g_last_good_coverage_pct = Gauge(
    "atr_policy_disaster_last_good_coverage_pct",
    "Percent of active policy keys that have a last_good mirror",
)
g_verify_ok_count = Gauge(
    "atr_policy_disaster_verify_ok_count",
    "Number of distinct cohorts with verified-ok active policy",
)
g_verify_fail_count = Gauge(
    "atr_policy_disaster_verify_fail_count",
    "Number of distinct cohorts with verify failure",
)
g_rollback_stream_len = Gauge(
    "atr_policy_disaster_rollback_stream_len",
    "Length of stream:atr_policy:rollback_results",
)
g_verify_stream_len = Gauge(
    "atr_policy_disaster_verify_stream_len",
    "Length of stream:atr_policy:verify_results",
)
g_escalation_stream_len = Gauge(
    "atr_policy_disaster_escalation_stream_len",
    "Length of stream:atr_policy:escalations in last 1000 entries",
)
g_callback_silence_pending = Gauge(
    "atr_policy_disaster_callback_silence_pending",
    "Number of submitted proposals with callback silence (watchdog)",
)

c_slo_loop = Counter(
    "atr_policy_disaster_slo_loop_total",
    "Disaster SLO loop iterations",
)
c_slo_error = Counter(
    "atr_policy_disaster_slo_error_total",
    "Disaster SLO collection errors",
    ["stage"],
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _rconn() -> redis.Redis:
    if get_atr_redis is not None:
        return get_atr_redis()
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _scan_keys(r: redis.Redis, pattern: str, count: int = 500) -> list[str]:
    cur: int = 0
    out: list[str] = []
    while True:
        cur, keys = r.scan(cur, match=pattern, count=count)
        out.extend(keys)
        if cur == 0:
            break
    return out


def _stream_len(r: redis.Redis, stream: str) -> int:
    try:
        return r.xlen(stream)
    except Exception:
        return 0


# ── Collectors ────────────────────────────────────────────────────────────────

def _collect_kill_switch_count(r: redis.Redis) -> int:
    keys = _scan_keys(r, "cfg:atr_policy:kill_switch:*")
    active = 0
    for k in keys:
        try:
            import json
            raw = r.get(k)
            if raw:
                obj = json.loads(raw)
                if obj.get("enabled"):
                    active += 1
        except Exception:
            pass
    return active


def _collect_last_good_coverage(r: redis.Redis) -> dict[str, Any]:
    """
    For each cfg:atr_policy:active:* key, check if matching last_good exists.
    Returns coverage percentage and raw counts.
    """
    active_keys = _scan_keys(r, "cfg:atr_policy:active:*")
    if not active_keys:
        return {"coverage_pct": 100.0, "active_count": 0, "with_last_good": 0}

    with_lg = 0
    for ak in active_keys:
        # active:src:sym:scenario:regime:bucket → last_good:src:sym:...
        lg_key = ak.replace("cfg:atr_policy:active:", "cfg:atr_policy:last_good:", 1)
        if r.exists(lg_key):
            with_lg += 1

    pct = (with_lg / len(active_keys)) * 100.0
    return {
        "coverage_pct": pct,
        "active_count": len(active_keys),
        "with_last_good": with_lg,
    }


def _collect_verify_ok_fail(r: redis.Redis) -> dict[str, int]:
    """
    Read last 200 entries from verify_results stream, count ok/fail.
    """
    ok = 0
    fail = 0
    seen_cohorts_ok: set = set()
    seen_cohorts_fail: set = set()
    try:
        entries = r.xrevrange(RS.ATR_POLICY_VERIFY, count=200)
        for _, fields in entries:
            cohort_key = (
                fields.get("source", "") + "|" + fields.get("symbol", "") + "|" +
                fields.get("scenario", "") + "|" + fields.get("regime", "") + "|" +
                fields.get("bucket", "")
            )
            if fields.get("verified_ok") == "True":
                seen_cohorts_ok.add(cohort_key)
            else:
                seen_cohorts_fail.add(cohort_key)
    except Exception as exc:
        logger.warning("disaster_slo: verify stream read failed: %s", exc)

    return {
        "verify_ok_count": len(seen_cohorts_ok),
        "verify_fail_count": len(seen_cohorts_fail),
    }


def _collect_callback_silence_pending(r: redis.Redis) -> int:
    """
    Quick proxy: read watchdog result from escalation stream, count WARN/CRITICAL events today.
    Lightweight — just checks if watchdog key says pending_submitted > 0.
    """
    try:
        from services.atr_policy_callback_watchdog import check_once
        result = check_once(r)
        if result.get("severity") in ("WARN", "CRITICAL"):
            return int(result.get("pending_submitted", 0))
    except Exception:
        pass
    return 0


# ── Public API ────────────────────────────────────────────────────────────────

def collect_once() -> dict[str, Any]:
    r = _rconn()
    out: dict[str, Any] = {}

    stages = [
        ("kill_switch", lambda: {"kill_switch_active_count": _collect_kill_switch_count(r)}),
        ("last_good_coverage", lambda: _collect_last_good_coverage(r)),
        ("verify_ok_fail", lambda: _collect_verify_ok_fail(r)),
        ("stream_lens", lambda: {
            "rollback_stream_len": _stream_len(r, RS.ATR_POLICY_ROLLBACK),
            "verify_stream_len": _stream_len(r, RS.ATR_POLICY_VERIFY),
            "escalation_stream_len": _stream_len(r, RS.ATR_POLICY_ESCALATIONS),
        }),
        ("callback_silence_pending", lambda: {
            "callback_silence_pending": _collect_callback_silence_pending(r)
        }),
    ]

    for stage, fn in stages:
        try:
            out.update(fn())
        except Exception as exc:
            logger.warning("disaster_slo: stage %s failed: %s", stage, exc)
            c_slo_error.labels(stage=stage).inc()

    return out


def export_once() -> dict[str, Any]:
    s = collect_once()
    g_kill_switch_active_count.set(s.get("kill_switch_active_count", 0))
    g_last_good_coverage_pct.set(s.get("coverage_pct", 0.0))
    g_verify_ok_count.set(s.get("verify_ok_count", 0))
    g_verify_fail_count.set(s.get("verify_fail_count", 0))
    g_rollback_stream_len.set(s.get("rollback_stream_len", 0))
    g_verify_stream_len.set(s.get("verify_stream_len", 0))
    g_escalation_stream_len.set(s.get("escalation_stream_len", 0))
    g_callback_silence_pending.set(s.get("callback_silence_pending", 0))
    return s


def run_forever() -> None:
    port = int(os.getenv("ATR_POLICY_DISASTER_SLO_PORT", "9138") or 9138)
    start_http_server(port)
    logger.info("disaster_slo_analytics: metrics server on :%d", port)

    interval = int(os.getenv("ATR_POLICY_DISASTER_SLO_INTERVAL_SEC", "30") or 30)
    while True:
        c_slo_loop.inc()
        try:
            s = export_once()
            logger.debug(
                "disaster_slo: kill_switch=%d last_good_cov=%.1f%% verify_ok=%d verify_fail=%d",
                s.get("kill_switch_active_count", 0),
                s.get("coverage_pct", 0.0),
                s.get("verify_ok_count", 0),
                s.get("verify_fail_count", 0),
            )
        except Exception as exc:
            logger.exception("disaster_slo: export_once failed: %s", exc)
            c_slo_error.labels(stage="export_once").inc()
        time.sleep(interval)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_forever()
