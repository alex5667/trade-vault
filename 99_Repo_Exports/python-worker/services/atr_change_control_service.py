from __future__ import annotations

import json
import logging
import time
from typing import Any

from services.analytics_db import get_conn
from services.atr_release_gate_service import build_scorecard

logger = logging.getLogger("atr_change_control")

def get_change(change_id: str) -> dict[str, Any] | None:
    sql = "SELECT * FROM atr_change_requests WHERE change_id = %s"
    with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
        cur.execute(sql, (change_id,))
        return cur.fetchone()

def _record_transition(cur, change_id: str, old_status: str, new_status: str, reason_code: str, meta: dict[str, Any]):
    cur.execute("""
        INSERT INTO atr_change_transitions (change_id, old_status, new_status, reason_code, transition_json)
        VALUES (%s, %s, %s, %s, %s)
    """, (change_id, old_status, new_status, reason_code, json.dumps(meta)))

def submit_change(
    change_id: str,
    change_type: str,
    scope_kind: str,
    title: str,
    author: str,
    owner: str,
    risk_level: str,
    reason_code: str,
    request_data: dict[str, Any],
    source: str = "",
    venue: str = "",
    symbol="",
    scenario: str = "",
    regime: str = "",
    risk_horizon_bucket: str = "",
    layer: str = "",
    policy_ver: int = 0,
) -> bool:
    """Submit a new formal change request."""
    now_ms = int(time.time() * 1000)
    initial_status = "DRAFT"

    sql = """
        INSERT INTO atr_change_requests (
            change_id, change_type, scope_kind, source, venue, symbol,
            scenario, regime, risk_horizon_bucket, layer, policy_ver,
            status, title, author, owner, risk_level, reason_code, request_json,
            created_at_ms, updated_at_ms
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON CONFLICT (change_id) DO NOTHING
    """
    params = (
        change_id, change_type, scope_kind, source, venue, symbol,
        scenario, regime, risk_horizon_bucket, layer, policy_ver,
        initial_status, title, author, owner, risk_level, reason_code,
        json.dumps(request_data), now_ms, now_ms
    )

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.rowcount > 0:
                _record_transition(cur, change_id, "NONE", initial_status, "SUBMIT_DRAFT", {})
            conn.commit()
            return cur.rowcount > 0
    except Exception as e:
        logger.error(f"Failed to submit change {change_id}: {e}")
        return False

def attach_replay_report(change_id: str, report: dict[str, Any]) -> bool:
    """Attach replay report and advance state to REPLAY_PASSED or REPLAY_FAILED."""
    now_ms = int(time.time() * 1000)
    status_passed = report.get("status") == "passed"
    new_status = "REPLAY_PASSED" if status_passed else "REPLAY_FAILED"

    try:
        with get_conn() as conn, conn.cursor() as cur:
            # check current state
            cur.execute("SELECT status FROM atr_change_requests WHERE change_id = %s FOR UPDATE", (change_id,))
            row = cur.fetchone()
            if not row:
                return False

            old_status = row[0]

            cur.execute("""
                INSERT INTO atr_change_artifacts (change_id, artifact_kind, artifact_json)
                VALUES (%s, 'replay_report', %s)
            """, (change_id, json.dumps(report)))

            cur.execute("""
                UPDATE atr_change_requests
                SET status = %s, updated_at_ms = %s
                WHERE change_id = %s
            """, (new_status, now_ms, change_id))

            _record_transition(cur, change_id, old_status, new_status, "REPLAY_EVALUATED", report)
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Failed to attach replay for {change_id}: {e}")
        return False

def approve_change(change_id: str, actor: str, note: str = "") -> bool:
    """Approve a change and advance state if approval policy is met."""
    now_ms = int(time.time() * 1000)
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM atr_change_requests WHERE change_id = %s FOR UPDATE", (change_id,))
            chg = cur.fetchone()
            if not chg:
                return False

            old_status = chg["status"]
            # Enforce prerequisites via Release Gate Scorecard
            try:
                scorecard = build_scorecard(change_id)
                if scorecard.get("decision") == "deny":
                    logger.warning(f"Cannot approve {change_id} due to Release Gate blockers: {scorecard.get('blockers')}")
                    return False
                if scorecard.get("decision") == "allow_with_override" and note != "OVERRIDE":
                    logger.warning(f"Cannot approve {change_id} without OVERRIDE note due to warnings: {scorecard.get('warnings')}")
                    return False
            except Exception as e:
                logger.error(f"Failed to evaluate release gate for {change_id}: {e}")
                return False

            cur.execute("""
                INSERT INTO atr_change_approvals (change_id, actor, action, note, action_json)
                VALUES (%s, %s, 'approve', %s, %s)
            """, (change_id, actor, note, '{}'))

            # Simple policy check for now: assuming 1 approval is enough to set APPROVED
            # Could be more complex
            new_status = "APPROVED"
            cur.execute("""
                UPDATE atr_change_requests
                SET status = %s, updated_at_ms = %s
                WHERE change_id = %s
            """, (new_status, now_ms, change_id))

            if new_status != old_status:
                _record_transition(cur, change_id, old_status, new_status, "APPROVED_BY_POLICY", {"actor": actor})
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Failed to approve {change_id}: {e}")
        return False

def start_rollout(change_id: str, manifest: dict[str, Any]) -> bool:
    """Apply rollout manifest and set to ROLLOUT_PENDING/ROLLED_OUT."""
    now_ms = int(time.time() * 1000)
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT status FROM atr_change_requests WHERE change_id = %s FOR UPDATE", (change_id,))
            row = cur.fetchone()
            if not row:
                return False
            old_status = row[0]
            if old_status != "APPROVED":
                logger.warning(f"Cannot rollout {change_id} from {old_status}")
                return False

            cur.execute("""
                INSERT INTO atr_change_artifacts (change_id, artifact_kind, artifact_json)
                VALUES (%s, 'rollout_manifest', %s)
            """, (change_id, json.dumps(manifest)))

            new_status = "ROLLED_OUT" # or MONITORING
            cur.execute("UPDATE atr_change_requests SET status = %s, updated_at_ms = %s WHERE change_id = %s",
                        (new_status, now_ms, change_id))
            _record_transition(cur, change_id, old_status, new_status, "ROLLOUT_STARTED", manifest)
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Rollout failed for {change_id}: {e}")
        return False

def pause_change(change_id: str, actor: str, note: str = "") -> bool:
    """Pause an active rollout."""
    now_ms = int(time.time() * 1000)
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT status FROM atr_change_requests WHERE change_id = %s FOR UPDATE", (change_id,))
            row = cur.fetchone()
            if not row:
                return False
            old_status = row[0]

            cur.execute("""
                INSERT INTO atr_change_approvals (change_id, actor, action, note, action_json)
                VALUES (%s, %s, 'pause', %s, %s)
            """, (change_id, actor, note, '{}'))

            new_status = "PAUSED"
            cur.execute("UPDATE atr_change_requests SET status = %s, updated_at_ms = %s WHERE change_id = %s",
                        (new_status, now_ms, change_id))
            _record_transition(cur, change_id, old_status, new_status, "MANUAL_PAUSE", {"actor": actor})
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Pause failed for {change_id}: {e}")
        return False

def request_rollback(change_id: str, manifest: dict[str, Any], actor: str = "system") -> bool:
    """Request rollback and append manifest."""
    now_ms = int(time.time() * 1000)
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT status FROM atr_change_requests WHERE change_id = %s FOR UPDATE", (change_id,))
            row = cur.fetchone()
            if not row:
                return False
            old_status = row[0]

            cur.execute("""
                INSERT INTO atr_change_artifacts (change_id, artifact_kind, artifact_json)
                VALUES (%s, 'rollback_manifest', %s)
            """, (change_id, json.dumps(manifest)))

            cur.execute("""
                INSERT INTO atr_change_approvals (change_id, actor, action, note, action_json)
                VALUES (%s, %s, 'rollback', 'rollback requested', %s)
            """, (change_id, actor, '{}'))

            new_status = "ROLLBACK_PENDING"
            cur.execute("UPDATE atr_change_requests SET status = %s, updated_at_ms = %s WHERE change_id = %s",
                        (new_status, now_ms, change_id))
            _record_transition(cur, change_id, old_status, new_status, "ROLLBACK_INITIATED", manifest)
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Rollback failed for {change_id}: {e}")
        return False

def complete_change(change_id: str, evidence: dict[str, Any]) -> bool:
    """Complete a change successfully."""
    now_ms = int(time.time() * 1000)
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT status FROM atr_change_requests WHERE change_id = %s FOR UPDATE", (change_id,))
            row = cur.fetchone()
            if not row:
                return False
            old_status = row[0]

            cur.execute("""
                INSERT INTO atr_change_artifacts (change_id, artifact_kind, artifact_json)
                VALUES (%s, 'evidence_pack', %s)
            """, (change_id, json.dumps(evidence)))

            new_status = "COMPLETED"
            cur.execute("UPDATE atr_change_requests SET status = %s, updated_at_ms = %s WHERE change_id = %s",
                        (new_status, now_ms, change_id))
            _record_transition(cur, change_id, old_status, new_status, "SLO_MET", evidence)
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Failed to complete {change_id}: {e}")
        return False
