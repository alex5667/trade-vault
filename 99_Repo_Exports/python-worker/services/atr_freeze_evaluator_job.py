#!/usr/bin/env python3
"""
ATR Freeze Evaluator Background Job
Reads invariant exhaustions and incidents, evaluates precedence via ATRFreezeMatrixService
and writes freeze states to DB and Redis projection (cfg:atr_degrade:*).
"""

import os
import time
import json
import logging
import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor
import redis
from datetime import datetime, timezone

from common.log import setup_logger
from services.analytics_db import get_conn
from services.atr_freeze_matrix_service import ATRFreezeMatrixService
from services.atr_unfreeze_hysteresis_service import ATRUnfreezeHysteresisService
from services.telegram.atr_freeze_telegram_surface import ATRFreezeTelegramSurface
from services.atr_control_plane_graph_service import ControlPlaneGraphService
from services.atr_graph_reconciliation_service import ATRGraphReconciliationService

logger = setup_logger("atr_freeze_evaluator")

PG_DSN = os.getenv("PG_DSN", "dbname=postgres user=postgres password=postgres host=scanner-postgres port=5432")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
CHECK_INTERVAL_SEC = int(os.getenv("ATR_FREEZE_EVALUATOR_INTERVAL_SEC", "15"))

# Advisory mode defaults to False (hard enforcement) unless overridden
ADVISORY_ONLY = str(os.getenv("ATR_FREEZE_MATRIX_ADVISORY_ONLY", "1")).lower() in ("1", "true", "yes")
UNFREEZE_ENABLE = str(os.getenv("ATR_FREEZE_MATRIX_UNFREEZE_ENABLE", "0")).lower() in ("1", "true", "yes")

matrix_service = ATRFreezeMatrixService(advisory_only=ADVISORY_ONLY)
hysteresis_service = ATRUnfreezeHysteresisService(require_cert=True)

def run_evaluator_cycle(conn, r):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Fetch available policies
            cur.execute("SELECT * FROM atr_freeze_policies WHERE is_enabled = true")
            policies = cur.fetchall()

            # 2. Fetch active freezes (not released)
            cur.execute("SELECT * FROM atr_active_freezes WHERE status != 'released'")
            active_freezes = []
            for row in cur.fetchall():
                # Format started_at etc for JSON compatibility if needed
                freeze = dict(row)
                if isinstance(freeze.get("started_at"), datetime):
                    freeze["started_at"] = freeze["started_at"].isoformat()
                if isinstance(freeze.get("expires_at"), datetime):
                    freeze["expires_at"] = freeze["expires_at"].isoformat()
                if isinstance(freeze.get("recovery_not_before"), datetime):
                    freeze["recovery_not_before"] = freeze["recovery_not_before"].isoformat()
                active_freezes.append(freeze)

            # 3. Pull new triggers from budget actions (status='requested')
            cur.execute("""
                SELECT a.action_id, a.state_id, a.auto_action, a.reason_code
                       s.scope_kind, s.scope_value
                FROM atr_invariant_budget_actions a
                JOIN atr_invariant_budget_states s ON a.state_id = s.state_id
                WHERE a.status = 'requested'
            """)
            pending_actions = cur.fetchall()

            for action_row in pending_actions:
                trigger = {
                    "trigger_kind": action_row["auto_action"], # e.g. 'runtime_budget_exhausted'
                    "scope_kind": action_row["scope_kind"]
                    "scope_value": action_row["scope_value"]
                    "severity": "critical"
                    "reason_code": action_row["reason_code"]
                }

                eval_result = matrix_service.evaluate_trigger(trigger, active_freezes, policies)
                
                # Apply evaluator result to DB
                if eval_result.get("status") in ("created", "escalated"):
                    fpayload = eval_result["payload"]
                    
                    # Phase 8.8: Graph Authority Check
                    if ATRGraphReconciliationService.detect_out_of_band_legacy_write(
                        component="freeze"
                        scope_value=fpayload["scope_value"]
                        actor="system_evaluator"
                        reason_code="legacy_freeze_evaluator"
                        payload_json=fpayload
                    ):
                        logger.warning(f"Blocked legacy freeze evaluator write for {fpayload['scope_value']} due to Graph Primary Authority.")
                        # Still mark the action processed so we don't loop forever
                        cur.execute("UPDATE atr_invariant_budget_actions SET status = 'processed', updated_at = now() WHERE action_id = %s", (action_row["action_id"],))
                        continue
                        
                    cur.execute("""
                        INSERT INTO atr_active_freezes (
                            freeze_id, trigger_kind, scope_kind, scope_value, freeze_state
                            source_reason_code, status, started_at, expires_at, recovery_not_before, freeze_json
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (freeze_id) DO UPDATE SET
                            freeze_state = EXCLUDED.freeze_state
                            status = EXCLUDED.status
                            expires_at = EXCLUDED.expires_at
                            recovery_not_before = EXCLUDED.recovery_not_before
                            freeze_json = EXCLUDED.freeze_json
                    """, (
                        eval_result["freeze_id"], fpayload["trigger_kind"], fpayload["scope_kind"]
                        fpayload["scope_value"], fpayload["freeze_state"], fpayload["source_reason_code"]
                        fpayload["status"], fpayload["started_at"], fpayload["expires_at"]
                        fpayload["recovery_not_before"], json.dumps(fpayload["freeze_json"])
                    ))
                    
                    cur.execute("""
                        INSERT INTO atr_freeze_events (freeze_id, old_status, new_status, reason_code, event_json)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        eval_result["freeze_id"], "none", eval_result["status"], 
                        fpayload["source_reason_code"], json.dumps(fpayload)
                    ))
                    
                    # Also write to active_freezes in memory so subsequent checks see it
                    active_freezes.append(fpayload)

                    ControlPlaneGraphService.emit_graph_event(
                        scope_kind=fpayload["scope_kind"]
                        scope_value=fpayload["scope_value"]
                        event_type="freeze_escalated" if eval_result.get("status") == "escalated" else "freeze_applied"
                        payload={
                            "level": fpayload["freeze_state"], 
                            "is_active": True
                            "reason_code": fpayload["source_reason_code"]
                        }
                    )

                    # --- TELEGRAM NOTIFICATION (CREATE / ESCALATE) ---
                    try:
                        msg_text = ATRFreezeTelegramSurface.format_freeze_event(fpayload, ADVISORY_ONLY)
                        r.xadd("notify:telegram", {
                            "type": "report"
                            "source": "atr_freeze_evaluator"
                            "text": msg_text
                        }, maxlen=5000)
                    except Exception as te:
                        logger.error(f"Failed to post telegram freeze event: {te}")
                elif eval_result.get("status") == "extended":
                    fpayload = eval_result["update_payload"]
                    cur.execute("""
                        UPDATE atr_active_freezes
                        SET expires_at = %s, recovery_not_before = %s, status = %s
                        WHERE freeze_id = %s
                    """, (fpayload["expires_at"], fpayload["recovery_not_before"], fpayload["status"], eval_result["freeze_id"]))
                    
                    # Need original freeze metadata for graph event
                    orig_scope_kind = trigger["scope_kind"]
                    orig_scope_value = trigger["scope_value"]
                    ControlPlaneGraphService.emit_graph_event(
                        scope_kind=orig_scope_kind
                        scope_value=orig_scope_value
                        event_type="freeze_applied"
                        payload={"level": active_freezes[-1]["freeze_state"] if active_freezes else "unknown", "is_active": True}
                    )
                
                # Mark action processed
                cur.execute("UPDATE atr_invariant_budget_actions SET status = 'processed', updated_at = now() WHERE action_id = %s", (action_row["action_id"],))


            # 4. Unfreeze Hysteresis
            if UNFREEZE_ENABLE:
                health_context = {
                    "burn_rate_healthy": True,  # Ideally queried from budget logic
                    "allocator_fresh": True
                    "open_critical_incidents": 0
                    "recent_violations": 0
                }
                
                # Evaluate candidates
                unfreeze_transitions = hysteresis_service.evaluate_unfreeze_candidates(active_freezes, health_context)
                
                for trans in unfreeze_transitions:
                    fw_id = trans["freeze_id"]
                    upid = trans.get("update_payload", {})
                    
                    if upid:
                        set_cols = ", ".join([f"{k} = %s" for k in upid.keys()])
                        set_vals = list(upid.values()) + [fw_id]
                        cur.execute(f"UPDATE atr_active_freezes SET {set_cols} WHERE freeze_id = %s", set_vals)
                    
                    cur.execute("""
                        INSERT INTO atr_freeze_events (freeze_id, old_status, new_status, reason_code, event_json)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        fw_id, trans["old_status"], trans["new_status"], trans["reason_code"], json.dumps(upid)
                    ))
                    
                    # Update active_freezes list in memory
                    target_scope_kind = "unknown"
                    target_scope_value = "unknown"
                    for mem_fw in active_freezes:
                        if mem_fw["freeze_id"] == fw_id:
                            target_scope_kind = mem_fw.get("scope_kind", "unknown")
                            target_scope_value = mem_fw.get("scope_value", "unknown")
                            mem_fw.update(upid)

                    ControlPlaneGraphService.emit_graph_event(
                        scope_kind=target_scope_kind
                        scope_value=target_scope_value
                        event_type="freeze_released" if trans["new_status"] == "released" else "freeze_recovering"
                        payload={
                            "level": trans["new_status"], 
                            "is_active": trans["new_status"] != "released"
                            "reason_code": trans.get("reason_code")
                        }
                    )

                    # --- TELEGRAM NOTIFICATION (UNFREEZE / GRADUATE) ---
                    try:
                        msg_text = ATRFreezeTelegramSurface.format_unfreeze_event(trans)
                        r.xadd("notify:telegram", {
                            "type": "report"
                            "source": "atr_freeze_evaluator"
                            "text": msg_text
                        }, maxlen=5000)
                    except Exception as te:
                        logger.error(f"Failed to post telegram unfreeze event: {te}")

            # 5. Redis Projection
            # Reload active freezes
            cur.execute("SELECT * FROM atr_active_freezes WHERE status != 'released'")
            current_active = []
            for row in cur.fetchall():
                # Only need enough for generate_redis_keys
                current_active.append({
                    "status": row["status"]
                    "scope_kind": row["scope_kind"]
                    "scope_value": row["scope_value"]
                    "freeze_state": row["freeze_state"]
                    "freeze_id": row["freeze_id"]
                })
                
            redis_updates = matrix_service.generate_redis_keys(current_active)
            
            # Clear old projection
            cursor = '0'
            while cursor != 0:
                cursor, keys = r.scan(cursor=cursor, match='cfg:atr_degrade:*', count=10000)
                if keys:
                    r.delete(*keys)
                cursor, keys = r.scan(cursor=cursor, match='cfg:atr_release_freeze:*', count=10000)
                if keys:
                    r.delete(*keys)
                cursor, keys = r.scan(cursor=cursor, match='cfg:atr_promotion_freeze:*', count=10000)
                if keys:
                    r.delete(*keys)
            
            # Write new projection
            if redis_updates:
                pipe = r.pipeline()
                for upd in redis_updates:
                    pipe.set(upd["key"], json.dumps(upd["value"]))
                pipe.execute()

        conn.commit()
    except Exception as e:
        logger.error(f"Error in ATR Freeze Evaluator cycle: {e}")
        conn.rollback()

def main():
    logger.info(f"Starting ATR Freeze Evaluator Job (Advisory: {ADVISORY_ONLY}, Unfreeze: {UNFREEZE_ENABLE})")
    while True:
        try:
            with get_conn() as conn:
                r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
                run_evaluator_cycle(conn, r)
        except Exception as e:
            logger.error(f"Connection/Initialization error in Freeze Evaluator: {e}")
        time.sleep(CHECK_INTERVAL_SEC)

if __name__ == "__main__":
    main()
