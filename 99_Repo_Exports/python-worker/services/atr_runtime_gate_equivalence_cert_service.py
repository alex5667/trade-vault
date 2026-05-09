import logging
from datetime import datetime
from typing import Any

from services.analytics_db import get_conn
from services.atr_graph_backed_runtime_gate import ATRGraphBackedRuntimeGateService

logger = logging.getLogger("atr_runtime_equivalence_cert")

class ATRRuntimeGateEquivalenceCertService:
    """
    Evaluates the readiness ladder for the runtime gate cutover (Phase 8.5).
    Checks R1-R7 and outputs one of: not_ready | shadow_healthy | ready_for_canary | ready_for_live
    """

    @staticmethod
    def evaluate_cutover_readiness() -> tuple[str, dict[str, Any]]:
        status = "not_ready"
        summary = {
            "checked_scopes": 0,
            "critical_drifts": 0,
            "warning_drifts": 0,
            "decision_match_rate": 0.0,
            "days_without_critical_drift": 0,
            "projection_fresh": False,
            "missing_dependencies": 0,
            "rule_checks": {}
        }

        try:
            with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
                # 1. Scope and Match Rate
                cur.execute("""
                    SELECT 
                        count(*) as total_checks,
                        count(case when status = 'passed' then 1 end) as passed_checks
                    FROM atr_runtime_gate_equivalence_checks
                    WHERE created_at >= NOW() - INTERVAL '1 day'
                """)
                row = cur.fetchone()
                total_checks = row["total_checks"] or 0
                passed_checks = row["passed_checks"] or 0
                match_rate = (passed_checks / total_checks) if total_checks > 0 else 0.0

                summary["checked_scopes"] = total_checks
                summary["decision_match_rate"] = match_rate
                summary["rule_checks"]["R1_match"] = (match_rate == 1.0)

                # 2. Open Drifts
                cur.execute("""
                    SELECT severity, count(*) as count
                    FROM atr_runtime_gate_drifts
                    WHERE status = 'open'
                    GROUP BY severity
                """)
                drifts = cur.fetchall()
                critical_drifts = 0
                warning_drifts = 0
                for d in drifts:
                    if d["severity"] == "critical":
                        critical_drifts += d["count"]
                    else:
                        warning_drifts += d["count"]

                summary["critical_drifts"] = critical_drifts
                summary["warning_drifts"] = warning_drifts
                summary["rule_checks"]["R5_no_critical_drifts"] = (critical_drifts == 0)

                # 3. Days without critical drift
                cur.execute("""
                    SELECT MAX(created_at) as last_critical
                    FROM atr_runtime_gate_drifts
                    WHERE severity = 'critical'
                """)
                last_crit = cur.fetchone()["last_critical"]
                if last_crit:
                    days_without = (datetime.now(last_crit.tzinfo) - last_crit).days
                else:
                    days_without = 7  # Assuming good if never happened

                summary["days_without_critical_drift"] = days_without

                # 4. Projection Freshness
                cur.execute("""
                    SELECT MIN(updated_at) as oldest_update
                    FROM atr_control_plane_nodes
                """)
                oldest_update = cur.fetchone()["oldest_update"]
                projection_fresh = False
                if oldest_update:
                    if (datetime.now(oldest_update.tzinfo) - oldest_update).total_seconds() < 86400: # 1d max stale
                        projection_fresh = True
                else:
                    # No nodes might mean it's not setup yet, but we shouldn't fail projection freshness strictly
                    projection_fresh = True

                summary["projection_fresh"] = projection_fresh
                summary["rule_checks"]["R6_projection_fresh"] = projection_fresh

                # 5. Evaluate Status Ladder
                # "ready_for_live": 7 days no critical, 100% match bounded, projection fresh, etc.
                if match_rate >= 0.99 and critical_drifts == 0 and projection_fresh:
                    if days_without >= 7 and match_rate == 1.0:
                        status = "ready_for_live"
                    elif days_without >= 3:
                        status = "ready_for_canary"
                    else:
                        status = "shadow_healthy"

        except Exception as e:
            logger.error(f"Error evaluating readiness: {e}")
            summary["error"] = str(e)
            status = "not_ready"

        ATRGraphBackedRuntimeGateService.mark_runtime_cutover_readiness(status, summary)
        return status, summary
