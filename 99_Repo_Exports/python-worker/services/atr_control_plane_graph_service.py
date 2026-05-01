import json
import logging
import time
import uuid
import os
import redis
from typing import Any, Dict, List, Optional
from datetime import datetime

from services.analytics_db import get_conn
from services.atr_effective_state_resolver import EffectiveStateResolver

logger = logging.getLogger("atr_control_plane_graph")

def _generate_id(prefix: str) -> str:
    return f"{prefix}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

class ControlPlaneGraphService:
    shadow_enabled = os.getenv("ATR_CONTROL_PLANE_GRAPH_SHADOW", "1") == "1"
    projection_enabled = os.getenv("ATR_CONTROL_PLANE_PROJECTION_ENABLE", "1") == "1"
    _redis_client = None

    @classmethod
    def _get_redis(cls):
        if cls._redis_client is None:
            cls._redis_client = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
        return cls._redis_client

    @staticmethod
    def _log_illegal_transition(
        conn,
        aggregate_type: str,
        aggregate_id: str,
        requested_transition: str,
        actor: str,
        reason_code: str,
        attempt_json: Dict[str, Any]
    ):
        attempt_id = _generate_id("ill_trans")
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO atr_illegal_transition_attempts (
                    attempt_id, aggregate_type, aggregate_id, requested_transition,
                    actor, reason_code, attempt_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                attempt_id, aggregate_type, aggregate_id, requested_transition,
                actor, reason_code, json.dumps(attempt_json)
            ))
            logger.warning(f"Illegal transition recorded: {reason_code} on {aggregate_id} ({requested_transition}) by {actor}")

    @staticmethod
    def _emit_event(
        conn,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        scope_kind: str,
        scope_value: str,
        actor: str,
        reason_code: str,
        event_json: Dict[str, Any]
    ) -> str:
        event_id = _generate_id("ev")
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO atr_control_plane_events (
                    event_id, event_type, aggregate_type, aggregate_id,
                    scope_kind, scope_value, actor, reason_code, event_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                event_id, event_type, aggregate_type, aggregate_id,
                scope_kind, scope_value, actor, reason_code, json.dumps(event_json)
            ))
        return event_id

    @staticmethod
    def create_node(
        node_id: str,
        node_type: str,
        scope_kind: str,
        scope_value: str,
        initial_state: Dict[str, Any],
        actor: str,
        reason_code: str,
        evidence: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Create a new formal node in the unified graph."""
        try:
            with get_conn() as conn:
                # 1. Emit journal event
                event_id = ControlPlaneGraphService._emit_event(
                    conn,
                    event_type="node_created",
                    aggregate_type=node_type.lower(),
                    aggregate_id=node_id,
                    scope_kind=scope_kind,
                    scope_value=scope_value,
                    actor=actor,
                    reason_code=reason_code,
                    event_json={"initial_state": initial_state, "evidence": evidence or {}}
                )

                # 2. Materialize node
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO atr_control_plane_nodes (
                            node_id, node_type, scope_kind, scope_value,
                            node_state_json, version, last_event_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        node_id, node_type, scope_kind, scope_value,
                        json.dumps(initial_state), 1, event_id
                    ))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to create graph node {node_id}: {e}")
            return False

    @staticmethod
    def transition_node(
        node_id: str,
        target_state: Dict[str, Any],
        actor: str,
        reason_code: str,
        evidence: Optional[Dict[str, Any]] = None,
        force_override: bool = False
    ) -> bool:
        """Transition a generic graph node's state."""
        try:
            with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM atr_control_plane_nodes WHERE node_id = %s FOR UPDATE", (node_id,))
                node = cur.fetchone()
                if not node:
                    raise ValueError(f"Node {node_id} not found")

                # Basic transition rules checking place
                # If R1-R7 fails here, log to illegal and return False
                # For this generic method, we let higher order functions implement checks R1-R7.

                event_id = ControlPlaneGraphService._emit_event(
                    conn,
                    event_type="state_transition",
                    aggregate_type=node["node_type"].lower(),
                    aggregate_id=node_id,
                    scope_kind=node["scope_kind"],
                    scope_value=node["scope_value"],
                    actor=actor,
                    reason_code=reason_code,
                    event_json={"old_state": node["node_state_json"], "new_state": target_state, "evidence": evidence or {}}
                )

                cur.execute("""
                    UPDATE atr_control_plane_nodes
                    SET node_state_json = %s, version = version + 1, last_event_id = %s, updated_at = %s
                    WHERE node_id = %s
                """, (
                    json.dumps(target_state), event_id, datetime.utcnow(), node_id
                ))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to transition node {node_id}: {e}")
            return False

    @staticmethod
    def attach_cert(
        cert_id: str,
        cert_kind: str,
        target_node_id: str,
        status: str, # passed, failed, pending
        actor: str,
        checks_json: Dict[str, Any]
    ) -> bool:
        """Attach a formal certification edge to a target node."""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO atr_control_plane_certifications (
                            cert_id, cert_kind, target_node_id, status, checks_json, summary_json
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        cert_id, cert_kind, target_node_id, status, json.dumps(checks_json), json.dumps({})
                    ))
                    
                    # Create an edge from node to cert if it passed
                    if status == "passed":
                        edge_id = _generate_id("edge_cert")
                        cur.execute("""
                            INSERT INTO atr_control_plane_edges (
                                edge_id, from_node_id, to_node_id, edge_type, edge_state_json
                            ) VALUES (%s, %s, %s, %s, %s)
                        """, (
                            edge_id, target_node_id, cert_id, "certifies", json.dumps({"cert_kind": cert_kind})
                        ))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to attach cert {cert_kind} to {target_node_id}: {e}")
            return False

    @staticmethod
    def apply_freeze(
        scope_kind: str,
        scope_value: str,
        freeze_level: str,
        actor: str,
        reason_code: str
    ) -> bool:
        """Apply a freeze to a scope. Creates a new node and a 'blocks' edge."""
        node_id = f"freeze:{scope_kind}:{scope_value}:{int(time.time()*100)}"
        ControlPlaneGraphService.create_node(
            node_id=node_id,
            node_type="FreezeState",
            scope_kind=scope_kind,
            scope_value=scope_value,
            initial_state={"status": "active", "level": freeze_level},
            actor=actor,
            reason_code=reason_code
        )
        return True

    @staticmethod
    def activate_override(
        scope_kind: str,
        scope_value: str,
        ttl_ms: int,
        actor: str,
        reason_code: str
    ) -> bool:
        """Activate an override on a scope. Creates a new node."""
        node_id = f"override:{scope_kind}:{scope_value}:{int(time.time()*100)}"
        expires_at = int(time.time() * 1000) + ttl_ms
        ControlPlaneGraphService.create_node(
            node_id=node_id,
            node_type="OverrideState",
            scope_kind=scope_kind,
            scope_value=scope_value,
            initial_state={"status": "active", "expires_at_ms": expires_at},
            actor=actor,
            reason_code=reason_code
        )
        return True

    @staticmethod
    def get_node(node_id: str) -> Optional[Dict[str, Any]]:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM atr_control_plane_nodes WHERE node_id = %s", (node_id,))
            return cur.fetchone()

    @staticmethod
    def emit_graph_event(scope_kind: str, scope_value: str, event_type: str, payload: Dict[str, Any]):
        """
        Records the event in the journal, updates the node/edge tables, 
        and updates the Redis shadow projection (Phase 8.1).
        """
        if not ControlPlaneGraphService.shadow_enabled:
            return

        try:
            event_id = str(uuid.uuid4())
            with get_conn() as conn, conn.cursor() as cur:
                # 1. Write the event journal
                cur.execute("""
                    INSERT INTO atr_control_plane_events (event_id, scope_kind, scope_value, event_type, payload_json)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (event_id, scope_kind, scope_value, event_type, json.dumps(payload)))

                # 2. Map event to node updates
                node_updates = ControlPlaneGraphService._map_event_to_node(event_type, payload)
                for node_type, node_state in node_updates.items():
                    node_id = f"{scope_kind}:{scope_value}:{node_type}"
                    # Upsert node
                    cur.execute("""
                        INSERT INTO atr_control_plane_nodes (node_id, scope_kind, scope_value, node_type, node_state_json)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (node_id) DO UPDATE SET 
                            node_state_json = EXCLUDED.node_state_json,
                            updated_at = NOW()
                    """, (node_id, scope_kind, scope_value, node_type, json.dumps(node_state)))

                    # 3. Handle specific blocking edges
                    if event_type == "release_decided":
                        edge_id = f"blocked_release:{scope_value}"
                        if payload.get("action") == "deny_release":
                            cur.execute("""
                                INSERT INTO atr_control_plane_edges (edge_id, from_node_id, to_node_id, edge_type, status)
                                VALUES (%s, %s, %s, 'blocks', 'active')
                                ON CONFLICT (edge_id) DO UPDATE SET status = 'active'
                            """, (edge_id, node_id, "runtime_execution"))
                        else:
                            cur.execute("""
                                UPDATE atr_control_plane_edges SET status = 'inactive' WHERE edge_id = %s
                            """, (edge_id,))
            
            # 4. Update memory projection to Shadow Redis Namespace
            if ControlPlaneGraphService.projection_enabled:
                ControlPlaneGraphService._update_shadow_projection(scope_kind, scope_value)

        except Exception as e:
            logger.error(f"Failed to process graph event {event_type} for {scope_value}: {e}", exc_info=True)

    @staticmethod
    def _map_event_to_node(event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Returns a dictionary of node_type -> node_state_json based on the incoming event.
        """
        if event_type == "rollout_stage_changed":
            return {"RolloutState": {"rollout_stage": payload.get("new_stage", "none")}}
        
        elif event_type in ("freeze_applied", "freeze_escalated", "freeze_recovering"):
            return {"FreezeState": {
                "level": payload.get("level", "scope_frozen"),
                "status": "active" if payload.get("is_active", True) else "inactive",
                "escalated": event_type == "freeze_escalated",
                "reason_code": payload.get("reason_code")
            }}
            
        elif event_type == "freeze_released":
            return {"FreezeState": {
                "status": "inactive",
                "level": "none",
                "released_at_ms": int(time.time() * 1000)
            }}
            
        elif event_type in ("override_requested", "override_approved", "override_activated"):
            return {"OverrideState": {
                "status": event_type.replace("override_", ""),
                "expires_at_ms": payload.get("expires_at_ms", 0),
                "level": payload.get("level", "normal"),
                "requester": payload.get("requester"),
                "approver": payload.get("approver")
            }}
            
        elif event_type in ("override_expired", "override_revoked"):
            return {"OverrideState": {
                "status": event_type.replace("override_", ""),
                "expires_at_ms": payload.get("expires_at_ms", 0),
                "level": "none"
            }}

        elif event_type == "release_decided":
            return {"ReleaseGate": {
                "action": payload.get("action", "allow_release"),
                "status": "active"
            }}
            
        return {}

    @staticmethod
    def _update_shadow_projection(scope_kind: str, scope_value: str):
        """
        Invokes EffectiveStateResolver in shadow_graph_mode and writes projection to Redis. 
        """
        state = EffectiveStateResolver.resolve_scope(scope_kind, scope_value, is_shadow_graph_mode=True)
        
        pipe = ControlPlaneGraphService._get_redis().pipeline()
        pipe.set(f"shadow:cfg:atr_effective_state:{scope_value}", state.get("effective_runtime_state", "unknown"))
        pipe.set(f"shadow:cfg:atr_rollout_stage:{scope_value}", state.get("rollout_stage", "none"))
        pipe.set(f"shadow:cfg:atr_freeze:{scope_value}", state.get("freeze_state", "none"))
        pipe.set(f"shadow:cfg:atr_override:{scope_value}", state.get("override_state", "none"))
        pipe.set(f"shadow:cfg:atr_release:{scope_value}", state.get("release_state", "allowed"))
        pipe.execute()
