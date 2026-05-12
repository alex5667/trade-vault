import logging
from datetime import datetime, timezone
from typing import Any

from services.analytics_db import get_conn

logger = logging.getLogger("atr_readiness_service")

class ControlPlaneReadinessService:
    @staticmethod
    def evaluate_cutover_readiness() -> dict[str, Any]:
        """
        Evaluate whether Phase 8.2 (Cutover) can safely be initiated.
        """
        score = 100.0
        blockers = []
        warnings = []

        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            # 1. Total nodes synced
            cur.execute("SELECT count(*) as c FROM atr_control_plane_nodes")
            total_nodes = cur.fetchone()["c"]  # type: ignore
            if total_nodes == 0:
                blockers.append("NO_GRAPH_NODES_PRESENT")
                score -= 100

            # 2. Total events synced
            cur.execute("SELECT count(*) as c FROM atr_control_plane_events")
            total_events = cur.fetchone()["c"]  # type: ignore
            if total_events < 10:
                warnings.append("LOW_GRAPH_EVENT_VOLUME")
                score -= 5

            # 3. Unresolved Drifts
            cur.execute("SELECT count(*) as c FROM atr_control_plane_drifts WHERE status = 'unresolved'")
            active_drifts = cur.fetchone()["c"]  # type: ignore
            if active_drifts > 0:
                blockers.append("UNRESOLVED_DRIFTS_ACTIVE")
                score -= 50

            # 4. Certification coverage
            cur.execute("""
                SELECT count(*) as uncertified
                FROM atr_control_plane_nodes n
                LEFT JOIN atr_control_plane_projection_certs c ON c.scope_value = n.scope_value AND c.status = 'passed'
                WHERE c.cert_id IS NULL
            """)
            uncertified = cur.fetchone()["uncertified"]  # type: ignore
            if uncertified > 0:
                warnings.append("SOME_NODES_UNCERTIFIED")
                score -= 10

        # Must be completely perfect to cutover (no unresolved drifts, enough nodes, etc)
        is_ready = len(blockers) == 0 and score >= 90.0

        return {
            "is_ready": is_ready,
            "readiness_score": score,
            "total_nodes_tracked": total_nodes,
            "total_drifts_active": active_drifts,
            "blockers": blockers,
            "warnings": warnings,
            "evaluated_at": datetime.now(timezone.utc).isoformat()
        }

if __name__ == "__main__":
    import json
    res = ControlPlaneReadinessService.evaluate_cutover_readiness()
    print(json.dumps(res, indent=2))
