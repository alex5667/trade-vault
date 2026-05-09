from __future__ import annotations

"""ATR Policy SRE Service — Phase 3.7.

Prometheus exporter for the policy control-plane:
  - pending backlog + oldest age (SLO-5)
  - decided queue size
  - active policy count
  - proposal->decision p95 latency (SLO-1)
  - approve->apply p95 latency (SLO-2)
  - reconcile freshness (SLO-4)
  - revoke/flip rate today (SLO-6)
  - confirm-token expiry rate (SLO-7)
  - callback denied count

Does NOT touch the trading hot-path. All SQL reads are bounded; Redis scans
use modest count= limits to avoid SCAN-flooding under high key count.

ENV:
  ATR_POLICY_SRE_METRICS_PORT      default 9137
  ATR_POLICY_SRE_SCRAPE_INTERVAL_SEC  default 30
  REDIS_URL
  ANALYTICS_DB_DSN / TRADES_DB_DSN
"""

import json
import logging
import os
import time
from typing import Any

import psycopg2
import psycopg2.extras
import redis
from prometheus_client import Counter, Gauge, start_http_server
import contextlib

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Connection helpers
# ──────────────────────────────────────────────────────────────────────────────

def _redis() -> redis.Redis:
    return redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        decode_responses=True,
    )


def _dsn() -> str:
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )


def _metrics_port() -> int:
    try:
        return int(os.getenv("ATR_POLICY_SRE_METRICS_PORT", "9137") or 9137)
    except Exception:
        return 9137


# ──────────────────────────────────────────────────────────────────────────────
# Prometheus gauges / counters
# ──────────────────────────────────────────────────────────────────────────────

g_pending_total = Gauge(
    "atr_policy_pending_total", "Pending policy proposals total"
)
g_pending_oldest_age_sec = Gauge(
    "atr_policy_pending_oldest_age_sec",
    "Oldest pending proposal age in seconds",
)
g_decided_total = Gauge(
    "atr_policy_decided_queue_total", "Decided queue size"
)
g_active_total = Gauge(
    "atr_policy_active_total", "Active policy total"
)
g_approve_to_apply_p95_sec = Gauge(
    "atr_policy_approve_to_apply_p95_sec",
    "P95 approve->apply latency seconds",
)
g_proposal_to_decision_p95_sec = Gauge(
    "atr_policy_proposal_to_decision_p95_sec",
    "P95 proposal->decision latency seconds",
)
g_revoke_today_total = Gauge(
    "atr_policy_revoke_today_total", "Total revokes today"
)
g_flip_today_total = Gauge(
    "atr_policy_flip_today_total", "Total policy flips today"
)
g_confirm_expired_today_total = Gauge(
    "atr_policy_confirm_expired_today_total",
    "Expired confirm tokens today",
)
g_callback_denied_today_total = Gauge(
    "atr_policy_callback_denied_today_total",
    "Denied Telegram callbacks today",
)
g_reconcile_last_success_age_sec = Gauge(
    "atr_policy_reconcile_last_success_age_sec",
    "Age since reconcile last success in seconds",
)

c_sre_loop_total = Counter(
    "atr_policy_sre_loop_total", "SRE service loop iterations"
)
c_sre_error_total = Counter(
    "atr_policy_sre_error_total",
    "SRE service collection errors",
    ["stage"],
)

# Phase 4 Metrics
atr_policy_bootstrap_restore_total = Counter(
    "atr_policy_bootstrap_restore_total", "Total keys restored from SQL during boot", ["kind"]
)
atr_policy_bootstrap_error_total = Counter(
    "atr_policy_bootstrap_error_total", "Errors during bootstrap sequence", ["stage"]
)
atr_policy_bootstrap_duration_sec = Gauge(
    "atr_policy_bootstrap_duration_sec", "Duration of bootstrap in seconds"
)
atr_policy_boot_mode_total = Counter(
    "atr_policy_boot_mode_total", "Boot mode executed", ["mode"]
)

# Phase 4 Metrics / Drift Checker
g_drift_total = Gauge(
    "atr_policy_state_drift_total", "Detected state drift between SQL and Redis", ["kind", "reason_code"]
)
g_repair_total = Gauge(
    "atr_policy_state_repair_total", "Repairs made to Redis from SQL", ["kind", "reason_code"]
)
g_drift_last_run_age_sec = Gauge(
    "atr_policy_state_drift_last_run_age_sec", "Age of last drift check run"
)
g_checker_error_total = Gauge(
    "atr_policy_state_checker_error_total", "Total errors in drift checker", ["stage"]
)
g_extra_redis_keys_total = Gauge(
    "atr_policy_state_extra_redis_keys_total", "Total extra redis keys", ["kind"]
)
g_orphan_queue_total = Gauge(
    "atr_policy_state_orphan_queue_total", "Total orphan queue entries", ["kind"]
)



# ──────────────────────────────────────────────────────────────────────────────
# Collection helpers
# ──────────────────────────────────────────────────────────────────────────────

def _scan_keys(r: redis.Redis, pattern: str, count: int = 500) -> list[str]:
    """SCAN with bounded iteration — avoids blocking under large key sets."""
    cur: int = 0
    out: list[str] = []
    while True:
        cur, keys = r.scan(cur, match=pattern, count=count)
        out.extend(keys)
        if cur == 0:
            break
    return out


def _pending_stats(r: redis.Redis) -> dict[str, Any]:
    """Count SUBMITTED proposals and find the oldest one."""
    ids: list[str] = list(r.smembers("queue:atr_policy:pending") or [])
    oldest_ms: int = 0
    now_ms: int = int(time.time() * 1000)

    for pid in ids:
        raw = r.get(f"cfg:proposals:atr_policy:{pid}")
        if not raw:
            continue
        try:
            obj = json.loads(raw)
            if (obj.get("status") or "") != "SUBMITTED":
                continue
            created = int(obj.get("created_at_ms") or 0)
            if created > 0 and (oldest_ms == 0 or created < oldest_ms):
                oldest_ms = created
        except Exception:
            continue

    return {
        "pending_total": len(ids),
        "pending_oldest_age_sec": (
            max(0, (now_ms - oldest_ms) // 1000) if oldest_ms > 0 else 0
        )
    }


def _queue_stats(r: redis.Redis) -> dict[str, Any]:
    return {
        "decided_total": len(list(r.smembers("queue:atr_policy:decided") or [])),
        "active_total": len(_scan_keys(r, "cfg:atr_policy:active:*")),
    }


def _audit_stats(conn: psycopg2.connection) -> dict[str, Any]:
    """Compute p95 latencies and daily counters from the audit table.

    All queries are bounded to the last 7 days and today respectively.
    Falls back gracefully if the table does not yet exist.
    """
    out: dict[str, Any] = {
        "proposal_to_decision_p95_sec": 0.0,
        "approve_to_apply_p95_sec": 0.0,
        "revoke_today_total": 0,
        "flip_today_total": 0,
        "confirm_expired_today_total": 0,
        "callback_denied_today_total": 0,
    }

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # SLO-1: proposal created → decision recorded
            cur.execute(
                """
                WITH x AS (
                    SELECT
                        EXTRACT(EPOCH FROM (
                            to_timestamp((decision_json->>'ts_ms')::bigint / 1000.0)
                            - to_timestamp((suggestion_json->>'created_at_ms')::bigint / 1000.0)
                        )) AS sec
                    FROM atr_promotion_policy_audit
                    WHERE created_at >= now() - interval '7 days'
                      AND decision_json IS NOT NULL
                      AND suggestion_json IS NOT NULL
                )
                SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY sec) AS p95
                FROM x
                WHERE sec IS NOT NULL AND sec >= 0
                """
            )
            row = cur.fetchone() or {}
            out["proposal_to_decision_p95_sec"] = float(row.get("p95") or 0.0)

            # SLO-2: decision approved → applied to active policy
            cur.execute(
                """
                WITH x AS (
                    SELECT
                        EXTRACT(EPOCH FROM (
                            to_timestamp((suggestion_json->>'applied_at_ms')::bigint / 1000.0)
                            - to_timestamp((decision_json->>'ts_ms')::bigint / 1000.0)
                        )) AS sec
                    FROM atr_promotion_policy_audit
                    WHERE created_at >= now() - interval '7 days'
                      AND applied = true
                      AND suggestion_json->>'applied_at_ms' IS NOT NULL
                )
                SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY sec) AS p95
                FROM x
                WHERE sec IS NOT NULL AND sec >= 0
                """
            )
            row = cur.fetchone() or {}
            out["approve_to_apply_p95_sec"] = float(row.get("p95") or 0.0)

            # SLO-6: revoke count and flip count today
            cur.execute(
                """
                SELECT
                    count(*) FILTER (
                        WHERE (decision_json->>'action') = 'REVOKE'
                    ) AS revoke_today_total,
                    count(*) FILTER (
                        WHERE (decision_json->>'action') IN ('APPROVE', 'REVOKE')
                    ) AS flip_today_total
                FROM atr_promotion_policy_audit
                WHERE created_at::date = current_date
                """
            )
            row = cur.fetchone() or {}
            out["revoke_today_total"] = int(row.get("revoke_today_total") or 0)
            out["flip_today_total"] = int(row.get("flip_today_total") or 0)
    except Exception as exc:
        logger.warning("atr_policy_sre_service: audit_stats query failed: %s", exc)

    return out


def _runtime_stats(r: redis.Redis) -> dict[str, Any]:
    """Read ephemeral Redis counters written by callback_worker and reconcile."""
    now_ms = int(time.time() * 1000)
    last_ts = r.get("atr_policy:reconcile:last_success_ts_ms")

    reconcile_age = 0
    if last_ts:
        try:
            reconcile_age = max(0, (now_ms - int(last_ts)) // 1000)
        except Exception:
            reconcile_age = 0

    confirm_expired = 0
    with contextlib.suppress(Exception):
        confirm_expired = int(r.get("atr_policy:confirm_expired_today_total") or 0)

    callback_denied = 0
    with contextlib.suppress(Exception):
        callback_denied = int(r.get("atr_policy:callback_denied_today_total") or 0)

    return {
        "reconcile_last_success_age_sec": reconcile_age,
        "confirm_expired_today_total": confirm_expired,
        "callback_denied_today_total": callback_denied,
    }

def _drift_stats(r: redis.Redis) -> dict[str, Any]:
    """Read ephemeral Redis hashes for drift checker stats."""
    now_ms = int(time.time() * 1000)
    last_ts = r.get("atr_policy:drift_check:last_run_ts_ms")
    age = max(0, (now_ms - int(last_ts)) // 1000) if last_ts else 0

    return {
        "drift_last_run_age_sec": age,
        "drift_total": r.hgetall("atr_policy:metrics:drift_total") or {},
        "repair_total": r.hgetall("atr_policy:metrics:repair_total") or {},
        "checker_error_total": r.hgetall("atr_policy:metrics:checker_error_total") or {},
        "extra_keys_total": r.hgetall("atr_policy:metrics:extra_keys_total") or {},
        "orphan_queue_total": r.hgetall("atr_policy:metrics:orphan_queue_total") or {},
    }

# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def collect_once() -> dict[str, Any]:
    """Collect all SRE metrics in one pass. Used by digest service too."""
    r = _redis()
    conn = psycopg2.connect(
        _dsn(), connect_timeout=5, application_name="atr_policy_sre_service"
    )
    try:
        out: dict[str, Any] = {}
        out.update(_pending_stats(r))
        out.update(_queue_stats(r))
        out.update(_audit_stats(conn))
        out.update(_runtime_stats(r))
        out.update(_drift_stats(r))
        return out
    finally:
        conn.close()


def export_once() -> dict[str, Any]:
    """Collect and publish all SRE metrics to Prometheus gauges."""
    s = collect_once()
    g_pending_total.set(s["pending_total"])
    g_pending_oldest_age_sec.set(s["pending_oldest_age_sec"])
    g_decided_total.set(s["decided_total"])
    g_active_total.set(s["active_total"])
    g_proposal_to_decision_p95_sec.set(s["proposal_to_decision_p95_sec"])
    g_approve_to_apply_p95_sec.set(s["approve_to_apply_p95_sec"])
    g_revoke_today_total.set(s["revoke_today_total"])
    g_flip_today_total.set(s["flip_today_total"])
    g_confirm_expired_today_total.set(s["confirm_expired_today_total"])
    g_callback_denied_today_total.set(s["callback_denied_today_total"])
    g_reconcile_last_success_age_sec.set(s["reconcile_last_success_age_sec"])

    g_drift_last_run_age_sec.set(s["drift_last_run_age_sec"])
    for k, v in s["drift_total"].items():
        kind, reason = k.split(":", 1)
        g_drift_total.labels(kind=kind, reason_code=reason).set(float(v))
    for k, v in s["repair_total"].items():
        kind, reason = k.split(":", 1)
        g_repair_total.labels(kind=kind, reason_code=reason).set(float(v))
    for k, v in s["checker_error_total"].items():
        g_checker_error_total.labels(stage=k).set(float(v))
    for k, v in s["extra_keys_total"].items():
        g_extra_redis_keys_total.labels(kind=k).set(float(v))
    for k, v in s["orphan_queue_total"].items():
        g_orphan_queue_total.labels(kind=k).set(float(v))

    return s


def run_forever() -> None:
    """Start HTTP /metrics server and scrape loop."""
    port = _metrics_port()
    start_http_server(port)
    logger.info("atr_policy_sre_service: metrics server started on :%d", port)

    interval = int(os.getenv("ATR_POLICY_SRE_SCRAPE_INTERVAL_SEC", "30") or 30)

    while True:
        c_sre_loop_total.inc()
        try:
            s = export_once()
            logger.debug(
                "atr_policy_sre_service: exported — pending=%d oldest_age=%ds"
                " decided=%d active=%d p2d_p95=%.0fs a2a_p95=%.0fs"
                " reconcile_age=%ds",
                s["pending_total"],
                s["pending_oldest_age_sec"],
                s["decided_total"],
                s["active_total"],
                s["proposal_to_decision_p95_sec"],
                s["approve_to_apply_p95_sec"],
                s["reconcile_last_success_age_sec"],
            )
        except Exception as exc:
            logger.exception("atr_policy_sre_service: export_once failed: %s", exc)
            c_sre_error_total.labels(stage="export_once").inc()
        time.sleep(interval)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_forever()
