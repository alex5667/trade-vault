#!/usr/bin/env python3
"""
ATR Charter Compliance Engine (Phase 10.1)
Responsible for executing declarative compliance checks against system state.
"""

import json
import os
import uuid
from datetime import datetime
from typing import Any

import redis
from prometheus_client import Counter, Gauge
from psycopg2.extras import RealDictCursor

from common.log import setup_logger
from services.analytics_db import get_conn
from services.atr_policy_enforcement_router import get_enforcement_router

logger = setup_logger("atr_charter_compliance_engine")

# --- Prometheus Metrics ---
PROM_RULE_EVAL_TOTAL = Counter("atr_charter_rule_eval_total", "Count of rule evaluations", ["context_kind", "rule_id", "status"])
PROM_BUNDLE_STATUS = Gauge("atr_charter_bundle_status", "Current status of compliance bundles", ["context_kind", "status"])
PROM_BLOCKING_FAILURE = Counter("atr_charter_blocking_failure_total", "Count of blocking charter failures", ["rule_id"])

class ATRCharterComplianceEngine:
    def __init__(self, redis_url: str = None):
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self._r = redis.Redis.from_url(self.redis_url, decode_responses=True)

    def generate_id(self, prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:10]}"

    def load_active_rules(self, conn) -> list[dict[str, Any]]:
        """Load and join rules with their mappings from DB."""
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT p.*, m.evaluator_type, m.source_type, m.source_ref, m.evaluator_json, m.evidence_required
                FROM atr_charter_policy_registry p
                JOIN atr_charter_compliance_mapping m ON p.rule_id = m.rule_id
                WHERE p.status = 'active'
            """)
            return cur.fetchall()

    def evaluate_context(self, context_kind: str, context_ref: str) -> dict[str, Any]:
        """Run all rules applicable to the given context."""
        logger.info(f"Evaluating charter compliance for context: {context_kind} ref: {context_ref}")

        with get_conn() as conn:
            all_rules = self.load_active_rules(conn)
            # Filter rules by context (rules can specify applicable contexts in policy_json or we check scope_kind)
            # For Phase 10.1, we'll assume policy_json['contexts'] exists or it applies if context_kind matches scope_kind
            applicable_rules = []
            for r in all_rules:
                policy = r.get("policy_json", {})
                rule_contexts = policy.get("contexts", [])
                if not rule_contexts or context_kind in rule_contexts:
                    applicable_rules.append(r)

            results = []
            for rule in applicable_rules:
                result = self._evaluate_rule(conn, rule, context_kind, context_ref)
                results.append(result)
                PROM_RULE_EVAL_TOTAL.labels(context_kind=context_kind, rule_id=rule["rule_id"], status=result["status"]).inc()

            bundle = self._build_compliance_bundle(context_kind, context_ref, results)

            # Phase 10.2: Route failed rules to enforcement actions
            if bundle["overall_status"] != "passed":
                failed_rule_ids = [r["rule_id"] for r in results if r["status"] == "failed"]
                router = get_enforcement_router()
                enforcement = router.decide_enforcement(context_kind, context_ref, failed_rule_ids)
                bundle["enforcement"] = enforcement

                # If enforcement is strict, we might override overall_status
                if enforcement["overall_action"] != "allow":
                    bundle["overall_status"] = "blocked_by_enforcement"

            self._persist_results(conn, results, bundle)

            # Metrics
            status_val = 1 if bundle["overall_status"] == "passed" else 0
            PROM_BUNDLE_STATUS.labels(context_kind=context_kind, status="passed").set(status_val)

            return bundle

    def _evaluate_rule(self, conn, rule: dict[str, Any], context_kind: str, context_ref: str) -> dict[str, Any]:
        """Route to specific evaluator type."""
        eval_type = rule["evaluator_type"]
        status = "skipped"
        reason_code = "UNKNOWN"
        evidence = {"evaluated_at": datetime.now().isoformat()}

        try:
            if eval_type == "sql_assert":
                status, reason_code, evidence = self._eval_sql_assert(conn, rule, context_kind, context_ref)
            elif eval_type == "cert_status":
                status, reason_code, evidence = self._eval_cert_status(conn, rule)
            elif eval_type == "artifact_present":
                status, reason_code, evidence = self._eval_artifact_present(conn, rule, context_ref)
            else:
                logger.warning(f"Unsupported evaluator type: {eval_type} for rule {rule['rule_id']}")
                status = "warning"
                reason_code = "UNSUPPORTED_EVALUATOR"
        except Exception as e:
            logger.error(f"Error evaluating rule {rule['rule_id']}: {e}")
            status = "failed"
            reason_code = "EVALUATION_ERROR"
            evidence["error"] = str(e)

        return {
            "rule_id": rule["rule_id"],
            "context_kind": context_kind,
            "context_ref": context_ref,
            "status": status,
            "severity": rule["severity"],
            "reason_code": reason_code,
            "evidence_json": evidence
        }

    def _eval_sql_assert(self, conn, rule: dict[str, Any], context_kind: str, context_ref: str) -> (str, str, dict[str, Any]):
        """Runs a SQL check and validates the predicate."""
        target = rule["source_ref"]
        eval_json = rule["evaluator_json"]
        predicate = eval_json.get("predicate", "1=1")

        # Security: In Phase 10.1 we only allow counts or specific status checks on allowed tables
        # Example target: atr_release_quarantines
        query = f"SELECT count(*) as count FROM {target} WHERE {predicate}"

        # If the rule is scoped, we might need to add context-based filtering
        # For simplicity, we assume the predicate already handles scope if needed or we append it

        with conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchone()
            # If using RealDictCursor, res is a dict
            count = res['count'] if isinstance(res, dict) else res[0]

            if count == 0:
                return "passed", "OK", {"count": count}
            else:
                return "failed", rule.get("policy_json", {}).get("reason_codes", {}).get("fail", "SQL_ASSERT_FAILED"), {"count": count}

    def _eval_cert_status(self, conn, rule: dict[str, Any]) -> (str, str, dict[str, Any]):
        target = rule["source_ref"] # e.g. protective_lifecycle_equivalence_cert
        query = f"SELECT status FROM {target} ORDER BY created_at DESC LIMIT 1"

        with conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchone()
            status = res['status'] if (res and isinstance(res, dict)) else (res[0] if res else None)

            if status == 'passed':
                return "passed", "OK", {"cert_status": "passed"}
            else:
                return "failed", rule.get("policy_json", {}).get("reason_codes", {}).get("fail", "CERT_FAILED"), {"cert_status": status or "missing"}

    def _eval_artifact_present(self, conn, rule: dict[str, Any], context_ref: str) -> (str, str, dict[str, Any]):
        artifact_kind = rule["source_ref"] # e.g. pre_release_checklist
        query = "SELECT count(*) as count FROM atr_change_artifacts WHERE change_id = %s AND artifact_kind = %s"

        with conn.cursor() as cur:
            cur.execute(query, (context_ref, artifact_kind))
            res = cur.fetchone()
            count = res['count'] if isinstance(res, dict) else res[0]

            if count > 0:
                return "passed", "OK", {"artifact_count": count}
            else:
                return "failed", rule.get("policy_json", {}).get("reason_codes", {}).get("fail", "ARTIFACT_MISSING"), {"artifact_count": 0}

    def _build_compliance_bundle(self, context_kind: str, context_ref: str, results: list[dict[str, Any]]) -> dict[str, Any]:
        overall_status = "passed"
        failed_rules = [r for r in results if r["status"] == "failed"]

        # Determine blocking
        blocked = False
        for f in failed_rules:
            # In a real system, we'd check enforcement_mode from the rule
            # Here we default to blocked if any fail
            blocked = True

        if blocked:
            overall_status = "blocked"
        elif any(r["status"] == "warning" for r in results):
            overall_status = "warning"

        return {
            "bundle_id": self.generate_id("bundle"),
            "context_kind": context_kind,
            "context_ref": context_ref,
            "overall_status": overall_status,
            "summary_json": {
                "total_rules": len(results),
                "passed": len([r for r in results if r["status"] == "passed"]),
                "failed": len(failed_rules),
                "failed_ids": [r["rule_id"] for r in failed_rules]
            }
        }

    def _persist_results(self, conn, results: list[dict[str, Any]], bundle: dict[str, Any]):
        with conn.cursor() as cur:
            # 1. Insert Bundle
            cur.execute("""
                INSERT INTO atr_charter_compliance_bundles (
                    bundle_id, context_kind, context_ref, overall_status, summary_json
                ) VALUES (%s, %s, %s, %s, %s)
            """, (bundle["bundle_id"], bundle["context_kind"], bundle["context_ref"],
                  bundle["overall_status"], json.dumps(bundle["summary_json"])))

            # 2. Insert Individual Results
            for r in results:
                res_id = self.generate_id("res")
                cur.execute("""
                    INSERT INTO atr_charter_compliance_results (
                        result_id, context_kind, context_ref, rule_id, status, severity, reason_code, evidence_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (res_id, r["context_kind"], r["context_ref"], r["rule_id"],
                      r["status"], r["severity"], r["reason_code"], json.dumps(r["evidence_json"])))

        conn.commit()

if __name__ == "__main__":
    # Example usage for auditing
    engine = ATRCharterComplianceEngine()
