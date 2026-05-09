from __future__ import annotations

"""ATR Policy Flip-Storm Guard — Phase 3.8 (Disaster Layer).

Detects when a single cohort receives too many APPROVE/REVOKE state changes
(flip storm) within a rolling 24-hour window.

On detection:
  - arms hard kill_switch for the cohort
  - publishes escalation
  - increments Prometheus counter
  - requires manual operator clear

Uses atr_promotion_policy_audit SQL table (read-only).

ENV:
  ATR_POLICY_FLIP_STORM_GUARD_ENABLE     default 1
  ATR_POLICY_FLIP_STORM_THRESHOLD        default 3   (flips in 24h)
  ATR_POLICY_FLIP_STORM_ADVISORY_ONLY    default 0
  ANALYTICS_DB_DSN / TRADES_DB_DSN
  REDIS_URL
"""

import json
import logging
import os
import time
from typing import Any

import psycopg2
import psycopg2.extras
import redis
from prometheus_client import Counter

logger = logging.getLogger(__name__)

STREAM_ESC = "stream:atr_policy:escalations"

# ── Prometheus ────────────────────────────────────────────────────────────────

c_flip_storm_total = Counter(
    "atr_policy_flip_storm_total",
    "ATR policy flip-storm detections",
    ["action"],                           # detected | kill_switch_armed | advisory
)
c_kill_switch_total = Counter(
    "atr_policy_kill_switch_total",
    "ATR policy kill_switch activations",
    ["reason_code"],
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _rconn() -> redis.Redis:
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


def _enable() -> bool:
    return os.getenv("ATR_POLICY_FLIP_STORM_GUARD_ENABLE", "1") == "1"


def _advisory_only() -> bool:
    return os.getenv("ATR_POLICY_FLIP_STORM_ADVISORY_ONLY", "0") == "1"


def _threshold() -> int:
    try:
        return int(os.getenv("ATR_POLICY_FLIP_STORM_THRESHOLD", "3") or 3)
    except Exception:
        return 3


def _publish(r: redis.Redis, payload: dict[str, Any]) -> None:
    try:
        r.xadd(STREAM_ESC, {k: str(v) for k, v in payload.items()}, maxlen=2000)
    except Exception as exc:
        logger.warning("flip_storm_guard: stream publish failed: %s", exc)


def _kill_switch_key(cohort: dict[str, Any]) -> str:
    return (
        f"cfg:atr_policy:kill_switch:"
        f"{cohort['source']}:{cohort['symbol']}:{cohort['scenario']}:"
        f"{cohort['regime']}:{cohort['risk_horizon_bucket']}"
    )


def _arm_kill_switch(r: redis.Redis, cohort: dict[str, Any], reason_code: str, now_ms: int) -> None:
    ks_key = _kill_switch_key(cohort)
    payload = {
        "enabled": True,
        "ts_ms": now_ms,
        "reason_code": reason_code,
        "cohort": cohort,
    }
    r.set(ks_key, json.dumps(payload, ensure_ascii=False, sort_keys=True))
    c_kill_switch_total.labels(reason_code=reason_code).inc()
    logger.error("flip_storm_guard: kill_switch armed — %s reason=%s", ks_key, reason_code)


# ── Core ──────────────────────────────────────────────────────────────────────

def detect_storm_cohorts() -> list[dict[str, Any]]:
    """
    Query audit table for cohorts with >= threshold flip actions in 24h.

    Returns list of cohort dicts with flip_count.
    """
    if not _enable():
        return []

    try:
        conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_flip_storm_guard")
    except Exception as exc:
        logger.error("flip_storm_guard: DB connect failed: %s", exc)
        return []

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                  source,
                  symbol,
                  scenario,
                  regime,
                  risk_horizon_bucket,
                  COUNT(*) FILTER (
                    WHERE (decision_json->>'action') IN ('APPROVE','REVOKE')
                  ) AS flips_24h
                FROM atr_promotion_policy_audit
                WHERE created_at > NOW() - INTERVAL '24 hours'
                  AND decision_json IS NOT NULL
                GROUP BY 1,2,3,4,5
                HAVING COUNT(*) FILTER (
                    WHERE (decision_json->>'action') IN ('APPROVE','REVOKE')
                  ) >= %(threshold)s
                """,
                {"threshold": _threshold()},
            )
            rows = cur.fetchall() or []
            return [dict(row) for row in rows]
    except Exception as exc:
        logger.error("flip_storm_guard: query failed: %s", exc)
        return []
    finally:
        conn.close()


def run_guard_once(r: redis.Redis | None = None) -> list[dict[str, Any]]:
    """
    Detect flip storms and arm kill_switch for each offending cohort.

    Returns list of action results.
    """
    if not _enable():
        return []

    r = r or _rconn()
    now_ms = int(time.time() * 1000)
    advisory = _advisory_only()

    storm_cohorts = detect_storm_cohorts()
    results: list[dict[str, Any]] = []

    for cohort in storm_cohorts:
        flip_count = int(cohort.get("flips_24h", 0))
        c_flip_storm_total.labels(action="detected").inc()

        cohort_key = {
            "source": (cohort.get("source", "")),
            "symbol": (cohort.get("symbol", "")),
            "scenario": (cohort.get("scenario", "")),
            "regime": (cohort.get("regime", "")),
            "risk_horizon_bucket": (cohort.get("risk_horizon_bucket", "")),
        }

        reason_code = "FLIP_STORM_KILL_SWITCH"

        if advisory:
            action_taken = "advisory_only"
            c_flip_storm_total.labels(action="advisory").inc()
            logger.warning(
                "flip_storm_guard: ADVISORY — cohort %s flips=%d threshold=%d",
                cohort_key, flip_count, _threshold(),
            )
        else:
            _arm_kill_switch(r, cohort_key, reason_code, now_ms)
            action_taken = "kill_switch_armed"
            c_flip_storm_total.labels(action="kill_switch_armed").inc()

        _publish(r, {
            "event": f"FLIP_STORM_{action_taken.upper()}",
            **cohort_key,
            "flip_count": flip_count,
            "threshold": _threshold(),
            "advisory_only": advisory,
            "ts_ms": now_ms,
        })

        results.append({
            "cohort": cohort_key,
            "flip_count": flip_count,
            "action_taken": action_taken,
            "reason_code": reason_code,
            "ts_ms": now_ms,
        })

        logger.warning(
            "flip_storm_guard: %s — cohort=%s flips=%d",
            action_taken, cohort_key, flip_count,
        )

    return results


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    results = run_guard_once()
    json.dump(results, sys.stdout, indent=2, ensure_ascii=False)
    print()
