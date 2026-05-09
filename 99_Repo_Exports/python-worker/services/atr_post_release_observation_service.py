import json
import logging
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg2.extras
from prometheus_client import Counter, Gauge

from services.analytics_db import get_conn as get_db_connection
from services.atr_model_config_drift_service import ATRModelConfigDriftService

logger = logging.getLogger("atr_post_release_observation_service")

# Metrics
OBSERVATION_TOTAL = Counter("atr_post_release_observation_total", "Total post-release observations", ["change_class", "status"])
CHECK_TOTAL = Counter("atr_post_release_check_total", "Total post-release checks evaluated", ["check_name", "status"])
PROMOTION_HOLD_TOTAL = Counter("atr_promotion_hold_total", "Total promotion holds created", ["severity", "status"])
PROMOTION_ELIGIBLE_TOTAL = Counter("atr_promotion_eligible_total", "Total changes promoted", ["change_class"])
PROMOTION_ROLLBACK_REVIEW_TOTAL = Counter("atr_promotion_rollback_review_total", "Total rollback reviews required")
OBSERVATION_DWELL_SEC = Gauge("atr_observation_dwell_sec", "Observation dwell duration", ["change_class"])

ATR_POST_RELEASE_OBSERVATION_ENABLE = os.getenv("ATR_POST_RELEASE_OBSERVATION_ENABLE", "1") == "1"
ATR_POST_RELEASE_OBSERVATION_ENFORCE = os.getenv("ATR_POST_RELEASE_OBSERVATION_ENFORCE", "0") == "1"

OBSERVATION_STATES = [
    "RELEASED",
    "OBSERVING",
    "OBSERVING_WITH_CONSTRAINTS",
    "PROMOTION_HOLD",
    "PROMOTION_ELIGIBLE",
    "ROLLBACK_REVIEW_REQUIRED"
]

DWELL_WINDOWS_HOURS = {
    "LOW_RISK_CONFIG": 2,
    "LOW_RISK_OBSERVABILITY": 2,
    "MEDIUM_POLICY": 12,
    "HIGH_GOVERNANCE": 24,
    "CRITICAL_RUNTIME_GATING": 12,
    "CRITICAL_EXECUTION_TOUCHING": 24,
    "PROTECTIVE_PATH_TOUCHING": 24
}

class ATRPostReleaseObservationService:
    @staticmethod
    def open_post_release_observation(change_id: str, change_class: str, target_scope: str) -> str | None:
        if not ATR_POST_RELEASE_OBSERVATION_ENABLE:
            return None

        observation_id = f"obs_{datetime.now(UTC).strftime('%Y_%m_%d')}_{uuid.uuid4().hex[:6]}"
        dwell_hours = DWELL_WINDOWS_HOURS.get(change_class, 24)
        started_at = datetime.now(UTC)
        observation_until = started_at + timedelta(hours=dwell_hours)
        summary = {"change_id": change_id, "dwell_hours": dwell_hours}

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO atr_post_release_observations 
                    (observation_id, change_id, change_class, target_scope, status, started_at, observation_until, summary_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (observation_id, change_id, change_class, target_scope, 'OBSERVING', started_at, observation_until, json.dumps(summary)))
                conn.commit()

        OBSERVATION_TOTAL.labels(change_class=change_class, status="OBSERVING").inc()
        logger.info(f"ATR Post-Release Observation\n\nChange: {change_id}\nClass: {change_class}\nScope: {target_scope}\nStatus: OBSERVING\nUntil: {observation_until.strftime('%Y-%m-%d %H:%M UTC')}")
        return observation_id

    @staticmethod
    def _submit_check(observation_id: str, check_name: str, status: str, details: dict) -> None:
        check_id = f"prchk_{observation_id}_{check_name}"
        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute("""
                    INSERT INTO atr_post_release_checks 
                    (check_id, observation_id, check_name, status, details_json)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (check_id) DO UPDATE SET status = EXCLUDED.status, details_json = EXCLUDED.details_json
                """, (check_id, observation_id, check_name, status, json.dumps(details)))
            conn.commit()
        CHECK_TOTAL.labels(check_name=check_name, status=status).inc()

    @staticmethod
    def evaluate_post_release_checks(observation_id: str, mock_telemetry: dict[str, Any] | None = None) -> None:
        """
        Polls telemetry for Signal/Gate, Dispatch, Execution, Protective, and Control-Plane health.
        Updates checks associated with observation.
        """
        if not ATR_POST_RELEASE_OBSERVATION_ENABLE:
            return

        with get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT * FROM atr_post_release_observations WHERE observation_id = %s", (observation_id,))
            obs = cur.fetchone()
            if not obs or obs['status'] in ['PROMOTION_ELIGIBLE', 'ROLLBACK_REVIEW_REQUIRED']:
                return

        # Fetch telemetry or use mock
        telem = mock_telemetry or {}

        # Evaluate Signal/Gate
        sg_details = telem.get("signal_gates", {})
        sg_failed = sg_details.get("book_stale_spike") or sg_details.get("atr_unavailable_spike") or sg_details.get("negative_ev_spike")
        ATRPostReleaseObservationService._submit_check(
            observation_id, "signal_gate_health", "failed" if sg_failed else "passed", sg_details
        )

        # Evaluate Execution
        ex_details = telem.get("execution", {})
        ex_failed = ex_details.get("mt5_connection_burst") or ex_details.get("requote_burst") or ex_details.get("slippage_shift")
        if ex_details.get("slippage_shift"):
            ATRModelConfigDriftService.detect_execution_cost_drift(
                scope_value=obs['target_scope'],
                slippage_ema=ex_details.get("slippage_ema", 0.0),
                approved_band=ex_details.get("approved_band", 10.0),
                details=ex_details
            )
        ATRPostReleaseObservationService._submit_check(
            observation_id, "execution_health", "failed" if ex_failed else "passed", ex_details
        )

        # Evaluate Protective
        pr_details = telem.get("protective", {})
        pr_failed = pr_details.get("protective_critical_drift") or pr_details.get("closeout_truth_broken")
        if pr_details.get("protective_critical_drift"):
            ATRModelConfigDriftService.detect_protective_outcome_drift(
                scope_value=obs['target_scope'],
                metric_name="critical_drift",
                deviation=pr_details.get("drift_value", 0.0),
                details=pr_details
            )
        ATRPostReleaseObservationService._submit_check(
            observation_id, "protective_health", "failed" if pr_failed else "passed", pr_details
        )

        # Evaluate Control Plane
        cp_details = telem.get("control_plane", {})
        cp_failed = cp_details.get("graph_consistency_cert_failed") or cp_details.get("quarantine_active")
        ATRPostReleaseObservationService._submit_check(
            observation_id, "control_plane_health", "failed" if cp_failed else "passed", cp_details
        )

    @staticmethod
    def open_promotion_hold(observation_id: str, scope_value: str, hold_reason_code: str, severity: str) -> str | None:
        if not ATR_POST_RELEASE_OBSERVATION_ENABLE:
            return None

        hold_id = f"ph_{uuid.uuid4().hex[:8]}"
        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute("""
                    INSERT INTO atr_promotion_holds 
                    (hold_id, observation_id, scope_value, hold_reason_code, severity, status, hold_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (hold_id, observation_id, scope_value, hold_reason_code, severity, "active", json.dumps({})))
            conn.commit()

        PROMOTION_HOLD_TOTAL.labels(severity=severity, status="active").inc()

        # Advance observation state based on severity
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM atr_post_release_observations WHERE observation_id = %s", (observation_id,))
                obs = cur.fetchone()
                if obs:
                    if severity == "critical" and "protective" in hold_reason_code:
                        new_state = "ROLLBACK_REVIEW_REQUIRED"
                        PROMOTION_ROLLBACK_REVIEW_TOTAL.inc()
                    elif severity == "critical":
                        new_state = "PROMOTION_HOLD"
                    else:
                        if obs['status'] not in ["PROMOTION_HOLD", "ROLLBACK_REVIEW_REQUIRED"]:
                            new_state = "OBSERVING_WITH_CONSTRAINTS"
                        else:
                            new_state = obs['status']

                    cur.execute("UPDATE atr_post_release_observations SET status = %s WHERE observation_id = %s", (new_state, observation_id))
                    conn.commit()
                    OBSERVATION_TOTAL.labels(change_class=obs['change_class'], status=new_state).inc()

                    if new_state == "PROMOTION_HOLD":
                        logger.warning(f"ATR Promotion Hold\n\nChange: {obs['change_id']}\nScope: {obs['target_scope']}\nStatus: PROMOTION_HOLD\nReasons: {hold_reason_code}")

        return hold_id

    @staticmethod
    def clear_promotion_hold(hold_id: str) -> None:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE atr_promotion_holds SET status = 'cleared', cleared_at = now() WHERE hold_id = %s RETURNING severity", (hold_id,))
                res = cur.fetchone()
                if res:
                    PROMOTION_HOLD_TOTAL.labels(severity=res[0], status="cleared").inc()
                conn.commit()

    @staticmethod
    def decide_promotion_status(observation_id: str) -> str:
        """
        Evaluate dwell window and checks, return and apply the appropriate promotion status.
        Status taxonomy: KEEP_OBSERVING, PROMOTION_HOLD, PROMOTION_ELIGIBLE, ROLLBACK_REVIEW_REQUIRED
        """
        if not ATR_POST_RELEASE_OBSERVATION_ENABLE:
            return "PROMOTION_ELIGIBLE"

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM atr_post_release_observations WHERE observation_id = %s", (observation_id,))
                obs = cur.fetchone()
                if not obs:
                    return "KEEP_OBSERVING"

                # Check active holds
                cur.execute("SELECT severity, hold_reason_code FROM atr_promotion_holds WHERE observation_id = %s AND status = 'active'", (observation_id,))
                active_holds = cur.fetchall()

                # Check execution or signal failures (even if not explicitly held yet)
                cur.execute("SELECT check_name FROM atr_post_release_checks WHERE observation_id = %s AND status = 'failed'", (observation_id,))
                failed_checks = cur.fetchall()

                # If protective or rollback review holds exist
                for h in active_holds:
                    if h['severity'] == "critical" and "protective" in h['hold_reason_code']:
                        return "ROLLBACK_REVIEW_REQUIRED"

                critical_holds = [h for h in active_holds if h['severity'] == "critical"]
                if critical_holds:
                    return "PROMOTION_HOLD"

                if failed_checks:
                    # If there are failed checks but no holds generated, we assume it's degraded
                    return "OBSERVING_WITH_CONSTRAINTS"

                warning_holds = [h for h in active_holds if h['severity'] == "warn" or h['severity'] == "error"]
                if warning_holds:
                    return "OBSERVING_WITH_CONSTRAINTS"

                now = datetime.now(UTC)
                if now < obs['observation_until']:
                    return "KEEP_OBSERVING"

                # All passed + dwell satisfied
                new_state = "PROMOTION_ELIGIBLE"
                if obs['status'] != new_state:
                    cur.execute("UPDATE atr_post_release_observations SET status = %s, completed_at = now() WHERE observation_id = %s", (new_state, observation_id))
                    conn.commit()
                    OBSERVATION_TOTAL.labels(change_class=obs['change_class'], status=new_state).inc()
                    PROMOTION_ELIGIBLE_TOTAL.labels(change_class=obs['change_class']).inc()

                    dwell_sec = (now - obs['started_at']).total_seconds()
                    OBSERVATION_DWELL_SEC.labels(change_class=obs['change_class']).set(dwell_sec)

                    logger.info(f"ATR Promotion Eligible\n\nChange: {obs['change_id']}\nClass: {obs['change_class']}\nScope: {obs['target_scope']}\nStatus: PROMOTION_ELIGIBLE")


                return new_state


def process_pending_observations() -> None:
    """
    Main entry point for the post-release observation daemon.
    Finds all active observations, evaluates checks, and updates promotion status.
    """
    service = ATRPostReleaseObservationService()

    with get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # Query all observations that are still in an observing state
        cur.execute("""
                SELECT observation_id FROM atr_post_release_observations 
                WHERE status IN ('OBSERVING', 'OBSERVING_WITH_CONSTRAINTS', 'PROMOTION_HOLD')
            """)
        active_ids = [row['observation_id'] for row in cur.fetchall()]

    if not active_ids:
        return

    for obs_id in active_ids:
        try:
            # 1. Run checks (in a real scenario, this would poll telemetry)
            service.evaluate_post_release_checks(obs_id)
            # 2. Decide if status should be promoted
            service.decide_promotion_status(obs_id)
        except Exception as e:
            logger.error(f"Failed to process observation {obs_id}: {e}")
