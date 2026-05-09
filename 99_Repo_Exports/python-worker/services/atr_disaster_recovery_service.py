import json
import logging
import os
from typing import Any

import psycopg2.extras
from prometheus_client import Counter, Summary

from services.analytics_db import get_conn as get_db_connection
from services.observability.metrics_registry import atr_restore_safe_mode as _atr_restore_safe_mode

logger = logging.getLogger("atr_dr_service")

# Metrics
DR_EVENTS_TOTAL = Counter("atr_dr_events_total", "Total DR events opened", ["dr_class", "status"])
RESTORE_STEPS_TOTAL = Counter("atr_restore_steps_total", "Total restore stages advanced", ["domain", "status"])
RESTORE_CERT_TOTAL = Counter("atr_restore_cert_total", "Total restore certifications", ["cert_kind", "status"])
RESTORE_DURATION_SEC = Summary("atr_restore_duration_sec", "Duration of a full DR restore", ["dr_class"])
RESTORE_SAFE_MODE = _atr_restore_safe_mode  # shared via metrics_registry to avoid duplicate registration
RESTORE_BLOCKED_RELEASE = Counter("atr_restore_blocked_release_total", "Blocked releases due to active DR event")

ATR_DR_POLICY_ENABLE = os.getenv("ATR_DR_POLICY_ENABLE", "1") == "1"
ATR_DR_POLICY_ENFORCE = os.getenv("ATR_DR_POLICY_ENFORCE", "0") == "1"

VALID_DR_CLASSES = [
    "WARM_RESTART",
    "PARTIAL_REDIS_LOSS",
    "PARTIAL_SQL_LOSS",
    "GRAPH_PROJECTION_LOSS",
    "EXECUTION_BRIDGE_LOSS",
    "PROTECTIVE_STATE_LOSS",
    "FULL_CONTROL_PLANE_COLD_START",
    "FULL_STACK_COLD_START"
]

VALID_STATUSES = [
    "DOWN",
    "BOOTSTRAPPING",
    "CONTROL_PLANE_RESTORED",
    "SIGNAL_PATH_RESTORED",
    "EXECUTION_RESTORED",
    "PROTECTIVE_RESTORED",
    "OBSERVING_AFTER_RESTORE",
    "LIMITED_RELEASE_ELIGIBLE",
    "NORMAL"
]

class ATRDisasterRecoveryService:
    @staticmethod
    def open_dr_event(dr_id: str, dr_class: str, scope_kind: str, scope_value: str, reason_code: str, details: dict[str, Any] = None) -> dict[str, Any]:
        """Initiates a formal disaster recovery event and drops system into safe mode."""
        if details is None:
            details = {}

        if dr_class not in VALID_DR_CLASSES:
             raise ValueError(f"Invalid dr_class: {dr_class}")

        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute("""
                    INSERT INTO atr_dr_events 
                    (dr_id, dr_class, scope_kind, scope_value, status, reason_code, dr_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (dr_id, dr_class, scope_kind, scope_value, 'opened', reason_code, json.dumps(details)))
            conn.commit()

        DR_EVENTS_TOTAL.labels(dr_class=dr_class, status="opened").inc()
        RESTORE_SAFE_MODE.labels(state="NO_NEW_RISK").set(1)

        # We start bootstrapping
        ATRDisasterRecoveryService.advance_restore_state(dr_id, "BOOTSTRAPPING")

        return {
            "dr_id": dr_id,
            "dr_class": dr_class,
            "status": "opened",
            "safe_mode": "NO_NEW_RISK"
        }

    @staticmethod
    def run_restore_stage(dr_id: str, step_id: str, domain: str, step_name: str, details: dict[str, Any] = None) -> None:
        """Records a specific restore operation running in a domain."""
        if details is None:
            details = {}

        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute("""
                    INSERT INTO atr_restore_steps 
                    (step_id, dr_id, domain, step_name, status, details_json)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (step_id, dr_id, domain, step_name, 'passed', json.dumps(details)))
            conn.commit()

        RESTORE_STEPS_TOTAL.labels(domain=domain, status="passed").inc()

    @staticmethod
    def evaluate_restore_cert(dr_id: str, cert_id: str, cert_kind: str, checks: dict[str, Any], status: str) -> None:
        """Evaluates and stores certification artifacts allowing progression through the ladder."""
        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute("""
                    INSERT INTO atr_restore_certifications 
                    (cert_id, dr_id, cert_kind, status, checks_json, summary_json, finished_at)
                    VALUES (%s, %s, %s, %s, %s, %s, now())
                """, (cert_id, dr_id, cert_kind, status, json.dumps(checks), json.dumps({"reviewed": True})))
            conn.commit()

        RESTORE_CERT_TOTAL.labels(cert_kind=cert_kind, status=status).inc()

    @staticmethod
    def advance_restore_state(dr_id: str, new_status: str) -> None:
        """Advances the overarching status of the DR ladder after certs pass."""
        if new_status not in VALID_STATUSES:
             raise ValueError(f"Invalid ladder state: {new_status}")

        # If moving to NORMAL, check for prior observation
        if new_status == "NORMAL":
            with get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT dr_json FROM atr_dr_events WHERE dr_id = %s", (dr_id,))
                row = cur.fetchone()
                if row:
                    if not (row['dr_json'] or {}).get("observed", False) and ATR_DR_POLICY_ENFORCE:
                        logger.error(f"Cannot progress {dr_id} to NORMAL without prior OBSERVING_AFTER_RESTORE.")
                        return

        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute("""
                    UPDATE atr_dr_events 
                    SET status = %s, 
                        dr_json = jsonb_set(COALESCE(dr_json, '{}'::jsonb), '{last_status}', %s)
                    WHERE dr_id = %s
                """, (new_status, json.dumps(new_status), dr_id))

            if new_status == "NORMAL":
                cur.execute("UPDATE atr_dr_events SET completed_at = now() WHERE dr_id = %s", (dr_id,))

            conn.commit()

        if new_status == "OBSERVING_AFTER_RESTORE":
            with get_db_connection() as conn, conn.cursor() as cur:
                # Record that observation started
                cur.execute("""
                        UPDATE atr_dr_events 
                        SET dr_json = jsonb_set(dr_json, '{observed}', 'true'::jsonb)
                        WHERE dr_id = %s
                    """, (dr_id,))
                conn.commit()

        if new_status == "NORMAL":
             RESTORE_SAFE_MODE.labels(state="NO_NEW_RISK").set(0)

    @staticmethod
    def get_active_dr_events() -> list[dict[str, Any]]:
        with get_db_connection() as conn:
             with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                 cur.execute("SELECT * FROM atr_dr_events WHERE status != 'NORMAL' AND status != 'completed' AND status != 'failed'")
                 return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def is_release_blocked_by_dr(target_scope: str = "global") -> dict[str, Any] | None:
        """Returns None if release is allowed, else returns dict with DR blocker info."""
        if not ATR_DR_POLICY_ENABLE:
             return None

        active_events = ATRDisasterRecoveryService.get_active_dr_events()
        if not active_events:
            return None

        for event in active_events:
            if event['scope_kind'] == 'global' or event['scope_kind'] == 'venue' or event['scope_value'] == target_scope:
                RESTORE_BLOCKED_RELEASE.inc()
                return {
                    "dr_id": event['dr_id'],
                    "dr_class": event['dr_class'],
                    "status": event['status']
                }
        return None
