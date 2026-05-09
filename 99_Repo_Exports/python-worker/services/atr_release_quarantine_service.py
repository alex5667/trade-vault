import json
import logging
import os
import uuid
from datetime import UTC, datetime, timedelta

import psycopg2.extras
from prometheus_client import Counter, Gauge

from services.analytics_db import get_conn as get_db_connection

logger = logging.getLogger("atr_release_quarantine_service")

# Metrics
QUARANTINE_TOTAL = Counter("atr_release_quarantine_total", "Total quarantines", ["quarantine_class", "status"])
QUARANTINE_BLOCK_TOTAL = Counter("atr_release_quarantine_block_total", "Total releases blocked", ["quarantine_class"])
QUARANTINE_EXIT_CHECK_TOTAL = Counter("atr_quarantine_exit_check_total", "Total exit checks", ["check_name", "status"])
QUARANTINE_WAIVER_TOTAL = Counter("atr_quarantine_waiver_total", "Total waivers granted or denied", ["status"])
QUARANTINE_DWELL_SEC = Gauge("atr_quarantine_dwell_sec", "Current dwell time", ["quarantine_class"])
QUARANTINE_ACTIVE_TOTAL = Gauge("atr_quarantine_active_total", "Active quarantines by severity", ["severity"])

ATR_RELEASE_QUARANTINE_ENABLE = os.getenv("ATR_RELEASE_QUARANTINE_ENABLE", "1") == "1"
ATR_RELEASE_QUARANTINE_ENFORCE = os.getenv("ATR_RELEASE_QUARANTINE_ENFORCE", "0") == "1"

QUARANTINE_CLASSES = [
    "SIGNAL_GATE_QUARANTINE",
    "EXECUTION_VENUE_QUARANTINE",
    "CONTROL_PLANE_QUARANTINE",
    "PROTECTIVE_PATH_QUARANTINE",
    "POST_TRADE_FEEDBACK_QUARANTINE"
]

QUARANTINE_STATES = [
    "NOT_QUARANTINED",
    "QUARANTINED",
    "RECOVERING_IN_QUARANTINE",
    "READY_FOR_REVIEW",
    "RELEASE_ELIGIBLE",
    "WAIVED"
]

DWELL_WINDOWS_HOURS = {
    "SIGNAL_GATE_QUARANTINE": 12,
    "EXECUTION_VENUE_QUARANTINE": 12,
    "CONTROL_PLANE_QUARANTINE": 24,
    "PROTECTIVE_PATH_QUARANTINE": 24,
    "POST_TRADE_FEEDBACK_QUARANTINE": 24
}

class ATRReleaseQuarantineService:
    @staticmethod
    def open_quarantine(incident_id: str,
                        quarantine_class: str,
                        scope_kind: str,
                        scope_value: str,
                        severity: str,
                        reason_code: str) -> str | None:
        if not ATR_RELEASE_QUARANTINE_ENABLE:
            return None

        if quarantine_class not in QUARANTINE_CLASSES:
            logger.warning(f"Invalid quarantine class: {quarantine_class}")
            return None

        quarantine_id = f"q_{datetime.now(UTC).strftime('%Y_%m_%d')}_{uuid.uuid4().hex[:6]}"
        dwell_hours = DWELL_WINDOWS_HOURS.get(quarantine_class, 24)
        not_before = datetime.now(UTC) + timedelta(hours=dwell_hours)
        summary = {"incident_id": incident_id, "dwell_hours": dwell_hours}

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO atr_release_quarantines 
                    (quarantine_id, incident_id, quarantine_class, scope_kind, scope_value, status, severity, reason_code, not_before_release_at, summary_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (quarantine_id, incident_id, quarantine_class, scope_kind, scope_value, 'QUARANTINED', severity, reason_code, not_before, json.dumps(summary)))
                conn.commit()

        QUARANTINE_TOTAL.labels(quarantine_class=quarantine_class, status="QUARANTINED").inc()
        QUARANTINE_ACTIVE_TOTAL.labels(severity=severity).inc()
        logger.info(f"Opened quarantine {quarantine_id} for {scope_value} due to {reason_code}")
        return quarantine_id

    @staticmethod
    def evaluate_quarantine_exit(quarantine_id: str) -> bool:
        """Evaluate if exit checks are passed and dwell satisfied, advance state if so."""
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM atr_release_quarantines WHERE quarantine_id = %s", (quarantine_id,))
                q = cur.fetchone()
                if not q or q['status'] in ['RELEASE_ELIGIBLE', 'WAIVED']:
                    return False

                # Dwell time check
                now = datetime.now(UTC)
                not_before = q['not_before_release_at']

                # Check for failed checks
                cur.execute("SELECT status FROM atr_quarantine_exit_checks WHERE quarantine_id = %s AND status = 'failed'", (quarantine_id,))
                has_failed_checks = cur.fetchone() is not None

                # Assume true if dwell passed and no explicit failed checks, and owner signed off.
                # In real scenario, would poll external states directly here or checks would be submitted by other jobs.
                if now >= not_before and not has_failed_checks:
                    # Determine next state
                    next_status = 'READY_FOR_REVIEW' if q['status'] != 'READY_FOR_REVIEW' else 'READY_FOR_REVIEW'
                    # we do not transition to RELEASE_ELIGIBLE automatically; human review must advance it
                    if q['status'] != next_status:
                        ATRReleaseQuarantineService.advance_quarantine_state(quarantine_id, next_status)
                        return True
        return False

    @staticmethod
    def advance_quarantine_state(quarantine_id: str, new_status: str) -> None:
        if new_status not in QUARANTINE_STATES:
            return

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT quarantine_class, severity, status FROM atr_release_quarantines WHERE quarantine_id = %s", (quarantine_id,))
                q = cur.fetchone()
                if not q:
                    return

                old_status = q['status']
                if new_status in ['RELEASE_ELIGIBLE', 'WAIVED'] and old_status not in ['RELEASE_ELIGIBLE', 'WAIVED']:
                    # Decrement active quarantine count
                    QUARANTINE_ACTIVE_TOTAL.labels(severity=q['severity']).dec()
                    released_at = datetime.now(UTC)
                    cur.execute("UPDATE atr_release_quarantines SET status = %s, released_at = %s WHERE quarantine_id = %s", (new_status, released_at, quarantine_id))
                else:
                    cur.execute("UPDATE atr_release_quarantines SET status = %s WHERE quarantine_id = %s", (new_status, quarantine_id))

                conn.commit()
                QUARANTINE_TOTAL.labels(quarantine_class=q['quarantine_class'], status=new_status).inc()
                logger.info(f"Quarantine {quarantine_id} advanced to {new_status}")

    @staticmethod
    def is_release_blocked_by_quarantine(target_scope: str) -> dict | None:
        if not ATR_RELEASE_QUARANTINE_ENABLE:
            return None

        with get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                    SELECT * FROM atr_release_quarantines 
                    WHERE status NOT IN ('RELEASE_ELIGIBLE', 'WAIVED', 'NOT_QUARANTINED')
                """)
            active_quarantines = cur.fetchall()

            for q in active_quarantines:
                if q['scope_value'] in target_scope:
                    QUARANTINE_BLOCK_TOTAL.labels(quarantine_class=q['quarantine_class']).inc()
                    return dict(q)
        return None

    @staticmethod
    def grant_quarantine_waiver(quarantine_id: str, approver: str, reason_code: str, ttl_sec: int) -> bool:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT quarantine_class, severity FROM atr_release_quarantines WHERE quarantine_id = %s", (quarantine_id,))
                q = cur.fetchone()
                if not q:
                    QUARANTINE_WAIVER_TOTAL.labels(status="denied").inc()
                    return False

                # Narrow policy: Forbidden for PROTECTIVE_PATH and EXECUTION critical
                if q['quarantine_class'] == "PROTECTIVE_PATH_QUARANTINE":
                    logger.warning(f"Waiver denied for {quarantine_id}: PROTECTIVE_PATH_QUARANTINE cannot be waived.")
                    QUARANTINE_WAIVER_TOTAL.labels(status="denied").inc()
                    return False

                if q['quarantine_class'] == "EXECUTION_VENUE_QUARANTINE" and q['severity'] == "critical":
                    logger.warning(f"Waiver denied for {quarantine_id}: Critical EXECUTION_VENUE_QUARANTINE cannot be waived.")
                    QUARANTINE_WAIVER_TOTAL.labels(status="denied").inc()
                    return False

                waiver_id = f"w_{uuid.uuid4().hex[:8]}"
                not_after = datetime.now(UTC) + timedelta(seconds=ttl_sec)

                cur.execute("""
                    INSERT INTO atr_quarantine_waivers
                    (waiver_id, quarantine_id, approver, reason_code, ttl_sec, not_after, waiver_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (waiver_id, quarantine_id, approver, reason_code, ttl_sec, not_after, json.dumps({})))

                conn.commit()

        ATRReleaseQuarantineService.advance_quarantine_state(quarantine_id, "WAIVED")
        QUARANTINE_WAIVER_TOTAL.labels(status="granted").inc()
        return True
