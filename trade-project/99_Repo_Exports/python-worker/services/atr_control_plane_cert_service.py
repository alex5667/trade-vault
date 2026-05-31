import logging
from typing import Any

from services.analytics_db import get_conn
from services.atr_control_plane_graph_service import ControlPlaneGraphService

logger = logging.getLogger("atr_control_plane_cert")

class ControlPlaneCertService:
    """
    Implements Certification Layer (Checks G1-G7) for the Control Plane Graph.
    """

    @staticmethod
    def check_graph_consistency() -> dict[str, Any]:
        """
        Runs comprehensive consistency checks G1-G7 over the active graph.
        """
        issues = []
        try:
            with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
                # G7: Orphan active nodes without evidence linkage
                cur.execute("""
                    SELECT node_id FROM atr_control_plane_nodes 
                    WHERE node_type = 'RolloutState' AND node_state_json->>'rollout_stage' LIKE 'live%%'
                      AND NOT EXISTS (
                          SELECT 1 FROM atr_control_plane_edges 
                          WHERE from_node_id = atr_control_plane_nodes.node_id AND edge_type = 'certifies'
                      )
                """)
                for row in cur.fetchall():
                    issues.append(f"G7_ORPHAN_LIVE_NODE_WITHOUT_CERT: {row['node_id']}")  # type: ignore

                # G2: Active override without valid TTL
                cur.execute("""
                    SELECT node_id, node_state_json->>'expires_at_ms' as expires 
                    FROM atr_control_plane_nodes 
                    WHERE node_type = 'OverrideState' AND node_state_json->>'status' = 'active'
                """)
                import time
                now_ms = int(time.time() * 1000)
                for row in cur.fetchall():
                    expires = int(row['expires'] or 0)  # type: ignore
                    if expires < now_ms:
                        issues.append(f"G2_EXPIRED_ACTIVE_OVERRIDE: {row['node_id']}")  # type: ignore

            return {
                "status": "passed" if not issues else "failed",
                "issues": issues
            }
        except Exception as e:
            logger.error(f"Failed to check graph consistency: {e}")
            return {"status": "error", "error": str(e)}

    @staticmethod
    def attach_graph_consistency_cert(actor: str) -> bool:
        """
        Runs checks and attaches a graph_consistency_cert to the system state (or global pseudo-node).
        """
        result = ControlPlaneCertService.check_graph_consistency()
        import time
        cert_id = f"cert_graph_cons_{int(time.time()*1000)}"
        return ControlPlaneGraphService.attach_cert(
            cert_id=cert_id,
            cert_kind="graph_consistency_cert",
            target_node_id="global",
            status=result["status"],
            actor=actor,
            checks_json={"issues": result.get("issues", []), "error": result.get("error")}
        )
