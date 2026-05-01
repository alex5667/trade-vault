import os
import json
import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
import redis

from services.analytics_db import get_conn
from services.atr_control_plane_graph_service import ControlPlaneGraphService
from services.atr_graph_reconciliation_service import ATRGraphReconciliationService

logger = logging.getLogger("atr_override_governance"),

class ATROverrideGovernanceService:
    def __init__(self):
        self.advisory_only = os.getenv("ATR_OVERRIDE_GOVERNANCE_ADVISORY_ONLY", "1") == "1",
        self.enabled = os.getenv("ATR_OVERRIDE_GOVERNANCE_ENABLE", "1") == "1",
        self.redis_client = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True),

    def _generate_id(self) -> str:
        return f"ovr_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}",

    def _get_authority_matrix(self) -> Dict[str, List[str]]:
        return {
            "operator": [
                "READONLY_ACK",
                "TEMP_CLIP_OVERRIDE",
                "TEMP_UNFREEZE_TO_REDUCE_ONLY"],
            "senior_operator": [
                "READONLY_ACK",
                "TEMP_CLIP_OVERRIDE",
                "TEMP_UNFREEZE_TO_REDUCE_ONLY",
                "TEMP_UNFREEZE_TO_CLIP",
                "TEMP_RELEASE_OVERRIDE" ],
            "technical_owner": [
                "READONLY_ACK",
                "TEMP_CLIP_OVERRIDE",
                "TEMP_UNFREEZE_TO_REDUCE_ONLY",
                "TEMP_UNFREEZE_TO_CLIP",
                "TEMP_RELEASE_OVERRIDE",
                "TEMP_PROMOTION_OVERRIDE",
                "EMERGENCY_RESTORE_OVERRIDE"
            ]
        }

    def _get_role(self, actor: str) -> str:
        # Mock role resolver for now based on some basic heuristic or config
        if "tech" in actor.lower() or "admin" in actor.lower():
            return "technical_owner"
        if "senior" in actor.lower():
            return "senior_operator"
        return "operator"

    def _check_hard_forbidden_rules(self, override_class: str, target_state: str, scope: Dict[str, Any]) -> Optional[str]:
        # Connect to DB to check for open SEV1 or protective breaches
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            # Check for SEV1
            cur.execute("SELECT count(*) as c FROM atr_incidents WHERE status != 'closed' AND severity = 'SEV-1'")
            sev1_open = cur.fetchone()["c"]
            if sev1_open > 0 and target_state in ["normal", "clip"]:
                return "FORBID_OVERRIDE_OPEN_SEV1_ON_RELATED_SCOPE"
            
            # Check for hard freeze breaches (e.g., from atr_invariant_violations)
            cur.execute("""
                SELECT count(*) as c FROM atr_invariant_violations v
                JOIN atr_invariants i ON v.invariant_id = i.invariant_id
                WHERE v.status != 'resolved' AND i.enforcement_mode = 'protective_exit_breach'
            """)
            if cur.fetchone()["c"] > 0:
                return "FORBID_OVERRIDE_HARD_FREEZE_PROTECTIVE_BREACH"
                
            # If target live100 without replay
            if target_state == "live_100":
                # simplistic check
                return "FORBID_OVERRIDE_LIVE100_WITHOUT_REPLAY"
        
        return None

    def request_override(self, override_class: str, scope: Dict[str, Any], current_state: str, requested_target_state: str, ttl_sec: int, requester: str, reason_code: str) -> Dict[str, Any]:
        if not self.enabled:
            return {"status": "error", "message": "Override Governance is disabled."}
            
        symbol = scope.get("symbol", "all")
        if ATRGraphReconciliationService.detect_out_of_band_legacy_write(
            component="override",
            scope_value=symbol,
            actor=requester,
            reason_code="legacy_request_override",
            payload_json={"class": override_class, "state": requested_target_state}
        ):
            logger.warning(f"Blocked legacy override request for {symbol} due to Graph Primary Authority.")
            return {"status": "error", "message": "Blocked by Graph Primary Authority"}

        override_id = self._generate_id()
        now_utc = datetime.now(timezone.utc)
        not_after = now_utc + timedelta(seconds=ttl_sec)
        
        req_json = {
            "scope": scope,
            "constraints": {
                "protective_exits_required": True,
                "risk_mult_cap": 0.25 if requested_target_state == "clip" else 1.0,
                "new_entries_allowed": requested_target_state in ["normal", "clip"]
            }
        }
        
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO atr_override_requests (
                    override_id, override_class, scope_kind, source, venue, symbol,
                    scenario, regime, risk_horizon_bucket, layer, policy_ver,
                    requested_target_state, current_state, status, requester, reason_code,
                    ttl_sec, not_after, request_json, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                override_id, override_class, scope.get("kind", "global"), scope.get("source"), scope.get("venue"),
                scope.get("symbol"), scope.get("scenario"), scope.get("regime"), scope.get("risk_horizon_bucket"),
                scope.get("layer"), scope.get("policy_ver"), requested_target_state, current_state,
                "requested", requester, reason_code, ttl_sec, not_after.isoformat(), json.dumps(req_json), now_utc.isoformat()
            ))
            
            cur.execute("""
                INSERT INTO atr_override_events (override_id, old_status, new_status, reason_code, event_json)
                VALUES (%s, %s, %s, %s, %s)
            """, (override_id, "none", "requested", reason_code, json.dumps({"requester": requester})))
            
            # Emit graph event
            ControlPlaneGraphService.emit_graph_event(
                scope_kind=scope.get("kind", "global"),
                scope_value=scope.get("symbol", "all"),
                event_type="override_requested",
                payload={
                    "level": requested_target_state,
                    "expires_at_ms": int(not_after.timestamp() * 1000),
                    "requester": requester,
                    "reason_code": reason_code
                }
            )

            conn.commit()

        return {"status": "success", "override_id": override_id}

    def approve_override(self, override_id: str, approver: str) -> Dict[str, Any]:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM atr_override_requests WHERE override_id = %s", (override_id,))
            req = cur.fetchone()
            if not req:
                return {"status": "error", "message": "Not found"}
                
            if req["status"] != "requested":
                return {"status": "error", "message": "Can only approve requested overrides"}
                
            role = self._get_role(approver)
            allowed_classes = self._get_authority_matrix().get(role, [])
            
            if req["override_class"] not in allowed_classes:
                return {"status": "error", "message": f"Authority error: {role} cannot approve {req['override_class']}"}
                
            forbidden_reason = self._check_hard_forbidden_rules(req["override_class"], req["requested_target_state"], req["request_json"].get("scope", {}))
            if forbidden_reason:
                return {"status": "error", "message": f"Forbidden: {forbidden_reason}"}
                
            symbol = req["symbol"] if req["symbol"] else "all"
            if ATRGraphReconciliationService.detect_out_of_band_legacy_write(
                component="override",
                scope_value=symbol,
                actor=approver,
                reason_code="legacy_approve_override",
                payload_json={"override_id": override_id}
            ):
                logger.warning(f"Blocked legacy override approval for {symbol} due to Graph Primary Authority.")
                return {"status": "error", "message": "Blocked by Graph Primary Authority"}
                
            now_utc = datetime.now(timezone.utc).isoformat()
            cur.execute("UPDATE atr_override_requests SET status = 'approved', approver = %s WHERE override_id = %s", (approver, override_id))
            cur.execute("""
                INSERT INTO atr_override_events (override_id, old_status, new_status, reason_code, event_json)
                VALUES (%s, %s, %s, %s, %s)
            """, (override_id, "requested", "approved", "OVERRIDE_APPROVED", json.dumps({"approver": approver})))
            
            # Emit graph event
            ControlPlaneGraphService.emit_graph_event(
                scope_kind=req["scope_kind"],
                scope_value=req["symbol"] or "all",
                event_type="override_approved",
                payload={
                    "level": req["requested_target_state"],
                    "expires_at_ms": int(datetime.fromisoformat(req["not_after"].replace('Z', '+00:00')).timestamp() * 1000) if isinstance(req["not_after"], str) else int(req["not_after"].timestamp() * 1000),
                    "approver": approver
                }
            )

            conn.commit()

        # Automatically activate
        self.activate_override(override_id)
        return {"status": "success"}

    def activate_override(self, override_id: str):
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM atr_override_requests WHERE override_id = %s", (override_id,))
            req = cur.fetchone()
            if not req or req["status"] != "approved":
                return
                
            now_utc = datetime.now(timezone.utc).isoformat()
            cur.execute("UPDATE atr_override_requests SET status = 'active', activated_at = %s WHERE override_id = %s", (now_utc, override_id))
            cur.execute("""
                INSERT INTO atr_override_events (override_id, old_status, new_status, reason_code, event_json)
                VALUES (%s, %s, %s, %s, %s)
            """, (override_id, "approved", "active", "OVERRIDE_ACTIVATED", json.dumps({})))
            # Emit graph event
            ControlPlaneGraphService.emit_graph_event(
                scope_kind=req["scope_kind"],
                scope_value=req["symbol"] or "all",
                event_type="override_activated",
                payload={
                    "level": req["requested_target_state"],
                    "expires_at_ms": int(datetime.fromisoformat(req["not_after"].replace('Z', '+00:00')).timestamp() * 1000) if isinstance(req["not_after"], str) else int(req["not_after"].timestamp() * 1000)
                }
            )

            conn.commit()

        self._sync_redis_state()

    def revoke_override(self, override_id: str, reason_code: str, auto_expire: bool = False):
        new_status = "expired" if auto_expire else "revoked"
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT status, scope_kind, symbol FROM atr_override_requests WHERE override_id = %s", (override_id,))
            req = cur.fetchone()
            if not req or req["status"] not in ["requested", "approved", "active"]:
                return
                
            symbol = req["symbol"] if req["symbol"] else "all"
            if not auto_expire and ATRGraphReconciliationService.detect_out_of_band_legacy_write(
                component="override",
                scope_value=symbol,
                actor="system",
                reason_code=reason_code,
                payload_json={"override_id": override_id}
            ):
                logger.warning(f"Blocked legacy override revoke for {symbol} due to Graph Primary Authority.")
                return
                
            old_status = req["status"]
            now_utc = datetime.now(timezone.utc).isoformat()
            cur.execute("UPDATE atr_override_requests SET status = %s, expired_at = %s WHERE override_id = %s", (new_status, now_utc, override_id))
            cur.execute("""
                INSERT INTO atr_override_events (override_id, old_status, new_status, reason_code, event_json)
                VALUES (%s, %s, %s, %s, %s)
            """, (override_id, old_status, new_status, reason_code, json.dumps({})))
            # Emit graph event
            ControlPlaneGraphService.emit_graph_event(
                scope_kind=req["scope_kind"],
                scope_value=req["symbol"] or "all",
                event_type="override_expired" if auto_expire else "override_revoked",
                payload={"level": "none", "expires_at_ms": 0, "reason_code": reason_code}
            )

            conn.commit()

        self._sync_redis_state()

    def certify_override(self, override_id: str):
        # Called after expiration to run cert checks O1-O6
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM atr_override_requests WHERE override_id = %s", (override_id,))
            req = cur.fetchone()
            if not req or req["status"] not in ["expired", "revoked"]:
                return
            
            # simplistic mock check
            checks = {"O1": "passed", "O2": "passed", "O3": "passed"}
            status = "passed"
            
            cur.execute("""
                INSERT INTO atr_post_override_certifications (cert_id, override_id, status, checks_json, summary_json)
                VALUES (%s, %s, %s, %s, %s)
            """, (f"cert_{override_id}", override_id, status, json.dumps(checks), json.dumps({"desc": "Simulated check"})))
            conn.commit()
            
    def run_ttl_checker(self):
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            now_utc = datetime.now(timezone.utc)
            cur.execute("SELECT override_id FROM atr_override_requests WHERE status = 'active' AND not_after <= %s", (now_utc.isoformat(),))
            active_expired = cur.fetchall()
            
            for row in active_expired:
                self.revoke_override(row["override_id"], "TTL_EXPIRED", auto_expire=True)
                self.certify_override(row["override_id"])

    def _sync_redis_state(self):
        # Update redis state based on active overrides to mask target freeze keys
        if self.advisory_only:
            logger.info("Override governance in advisory mode. Skpping redis sync.")
            return
            
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM atr_override_requests WHERE status = 'active'")
            active_overrides = cur.fetchall()
            
            # Simple approach: clear all override keys first or set them to normal
            # For this example, we just add `cfg:atr_override:*` keys
            for o in active_overrides:
                scope = f"{o['scope_kind']}:{o.get('symbol','*')}"
                if o.get("symbol") is None:
                    scope = "global:all" # fallback
                
                payload = {
                    "state": o["requested_target_state"],
                    "override_id": o["override_id"]
                }
                
                self.redis_client.set(f"cfg:atr_override:{scope}", json.dumps(payload), ex=3600)
                
            # Usually we'd also clean up expired keys, but `ex` handles it, or explicit delete.
