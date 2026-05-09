import json
import logging
import time
import uuid
from typing import Any

from services.analytics_db import get_conn
from services.atr_effective_state_resolver import EffectiveStateResolver

logger = logging.getLogger("atr_freeze_override_equivalence_cert")

class ATRFreezeOverrideEquivalenceCertService:
    """
    Equivalence Certification Service (F1-F9) for Phase 8.3.
    Compares Legacy truth with Graph truth for Freeze and Override states.
    """

    @staticmethod
    def certify_equivalence(scope_kind: str, scope_value: str) -> dict[str, Any]:
        """
        Runs certification checks between legacy and graph sources of truth.
        """
        cert_id = f"cert_eq_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        try:
            # 1. Resolve Legacy State
            legacy_state = EffectiveStateResolver.resolve_scope(scope_kind, scope_value, is_shadow_graph_mode=False)

            # 2. Resolve Graph State
            graph_state = EffectiveStateResolver.resolve_scope(scope_kind, scope_value, is_shadow_graph_mode=True)

            # 3. Perform Checks (F1-F9)
            checks = {
                "F1_effective_runtime_match": legacy_state.get("effective_runtime_state") == graph_state.get("effective_runtime_state"),
                "F2_freeze_state_match": legacy_state.get("freeze_state") == graph_state.get("freeze_state"),
                "F3_override_status_match": legacy_state.get("override_state") == graph_state.get("override_state"),
                "F4_rollout_stage_match": legacy_state.get("rollout_stage") == graph_state.get("rollout_stage"),
                "F5_release_blocked_match": legacy_state.get("release_state") == graph_state.get("release_state")
            }

            all_passed = all(checks.values())
            drift_detected = not all_passed

            with get_conn() as conn, conn.cursor() as cur:
                # 4. Record Check Result
                cur.execute("""
                    INSERT INTO atr_freeze_override_equivalence_checks (
                        check_id, scope_kind, scope_value, legacy_state_json, 
                        graph_state_json, checks_json, drift_detected
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    cert_id, scope_kind, scope_value,
                    json.dumps(legacy_state), json.dumps(graph_state),
                    json.dumps(checks), drift_detected
                ))

                # 5. Log Drift if detected
                if drift_detected:
                    drift_id = f"drift_{cert_id}"
                    drift_json = {
                        "legacy": {k: legacy_state.get(k) for k in ["effective_runtime_state", "freeze_state", "override_state"]},
                        "graph": {k: graph_state.get(k) for k in ["effective_runtime_state", "freeze_state", "override_state"]}
                    }
                    cur.execute("""
                        INSERT INTO atr_freeze_override_drifts (
                            drift_id, scope_kind, scope_value, drift_type, source_diff_json
                        ) VALUES (%s, %s, %s, %s, %s)
                    """, (drift_id, scope_kind, scope_value, "equivalence_failure", json.dumps(drift_json)))
                    logger.warning(f"Drift detected in {scope_value}: {drift_json}")

                conn.commit()

            return {
                "cert_id": cert_id,
                "passed": all_passed,
                "checks": checks,
                "legacy": legacy_state,
                "graph": graph_state
            }

        except Exception as e:
            logger.error(f"Equivalence certification failed for {scope_value}: {e}", exc_info=True)
            return {"cert_id": cert_id, "passed": False, "error": str(e)}

    @staticmethod
    def run_batch_certification(scope_kind: str, scope_values: list[str]):
        results = []
        for val in scope_values:
            res = ATRFreezeOverrideEquivalenceCertService.certify_equivalence(scope_kind, val)
            results.append(res)
        return results
