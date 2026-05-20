import json
import logging
from enum import StrEnum
from typing import Any

from prometheus_client import Counter, Gauge
from psycopg2.extras import RealDictCursor

from services.analytics_db import get_conn

logger = logging.getLogger(__name__)

# Metrics
ATR_PROGRAM_CLOSURE_PACKAGES_TOTAL = Counter(
    "atr_program_closure_packages_total",
    "Total closure packages built",
    ["status", "verdict"]
)

ATR_PROGRAM_HANDOFFS_TOTAL = Counter(
    "atr_program_handoffs_total",
    "Total handoffs recorded",
    ["domain", "status"]
)

ATR_PROGRAM_RESIDUAL_BACKLOG_TOTAL = Counter(
    "atr_program_residual_backlog_total",
    "Total backlog items recorded",
    ["domain", "backlog_class", "status"]
)

ATR_PROGRAM_CLOSURE_BLOCK_TOTAL = Counter(
    "atr_program_closure_block_total",
    "Total times closure was blocked",
    ["reason_code"]
)

ATR_PROGRAM_STABILIZATION_WINDOW_TOTAL = Gauge(
    "atr_program_stabilization_window_total",
    "Current status of stabilization window",
    ["status"]
)


class ProgramClosureVerdict(StrEnum):
    PROGRAM_CLOSED = "PROGRAM_CLOSED"
    CLOSED_WITH_RESIDUAL_BACKLOG = "CLOSED_WITH_RESIDUAL_BACKLOG"
    HOLD_OPEN = "HOLD_OPEN"
    REJECT_CLOSE = "REJECT_CLOSE"

class ProgramClosureStatus(StrEnum):
    DRAFT = "draft"
    READY = "ready"
    SIGNED = "signed"
    ACTIVE = "active"
    REJECTED = "rejected"

class DomainHandoffStatus(StrEnum):
    PENDING = "pending"
    ACCEPTED = "accepted"
    REJECTED = "rejected"

class ResidualBacklogClass(StrEnum):
    BLOCKING = "blocking"
    NON_BLOCKING = "non_blocking"
    HYGIENE = "hygiene"
    DEFERRED_EXPERIMENT = "deferred_experiment"

class ATRProgramClosureService:
    def __init__(self):
        self.required_domains = [
            "signal_and_gates",
            "dispatch_and_runtime",
            "execution",
            "protective_lifecycle",
            "control_plane_governance",
            "dr_replay_archive"]

    def check_stabilization_window(self, conn, required_days: int = 14) -> dict[str, Any]:
        """
        Check if the stabilization window is passed.
        Requires a streak of GREEN scorecards after the last Go-Live sign-off.
        """
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 1. Get latest signed GO_LIVE package
                cur.execute("""
                    SELECT signed_at FROM atr_go_live_readiness_packages
                    WHERE package_status = 'signed' AND verdict IN ('GO_LIVE', 'GO_LIVE_WITH_CONSTRAINTS')
                    ORDER BY signed_at DESC LIMIT 1
                """)
                golive = cur.fetchone()
                if not golive or not golive['signed_at']:
                    ATR_PROGRAM_STABILIZATION_WINDOW_TOTAL.labels(status="no_golive").set(0)
                    return {"passed": False, "reason": "NO_SIGNED_GO_LIVE", "days_stable": 0}

                signed_at = golive['signed_at']

                # 2. Find green weeks streak after signed_at
                cur.execute("""
                    SELECT week_start, overall_status FROM atr_weekly_operating_scorecards
                    WHERE week_start >= %s::date
                    ORDER BY week_start ASC
                """, (signed_at,))
                scorecards = cur.fetchall()

                if not scorecards:
                    ATR_PROGRAM_STABILIZATION_WINDOW_TOTAL.labels(status="no_scorecards").set(0)
                    return {"passed": False, "reason": "NO_SCORECARDS_AFTER_GOLIVE", "days_stable": 0}

                streak_days = 0
                for i, sc in enumerate(scorecards):
                    if sc['overall_status'] != 'GO':
                        # Streak broken
                        ATR_PROGRAM_STABILIZATION_WINDOW_TOTAL.labels(status="broken").set(0)
                        return {
                            "passed": False,
                            "reason": f"STREAK_BROKEN_AT_{sc['week_start']}",
                            "days_stable": streak_days,
                        }
                    # Roughly count days (7 per week)
                    streak_days += 7

                passed = streak_days >= required_days
                ATR_PROGRAM_STABILIZATION_WINDOW_TOTAL.labels(status="passed" if passed else "insufficient").set(streak_days)

                return {
                    "passed": passed,
                    "reason": "STABILIZATION_IN_PROGRESS" if not passed else "WINDOW_PASSED",
                    "days_stable": streak_days,
                    "required_days": required_days,
                    "golive_date": signed_at.isoformat()
                }
        except Exception as e:
            logger.error(f"Error checking stabilization window: {e}")
            return {"passed": False, "reason": f"ERROR: {str(e)}", "days_stable": 0}

    def auto_triage_closure_evidence(self, conn) -> dict[str, Any]:
        """
        Automatically collect evidence for program closure.
        """
        evidence = {
            "charter_active": False,
            "enforcement_map_active": False,
            "critical_coverage_gaps": 1,
            "e2e_acceptance_passed": False,
            "go_live_signed": False,
            "critical_quarantine_active": True,
            "stabilization_passed": False
        }

        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 1. Charter
                cur.execute("SELECT count(*) FROM atr_operating_charters WHERE status = 'active'")
                evidence["charter_active"] = cur.fetchone()['count'] > 0

                # 2. Enforcement
                cur.execute("SELECT count(*) FROM atr_charter_policy_registry WHERE status = 'active'")
                evidence["enforcement_map_active"] = cur.fetchone()['count'] > 0

                # 3. Gaps
                cur.execute("SELECT count(*) FROM atr_policy_gap_closure_matrix WHERE status IN ('open', 'remediation') AND priority = 'P0'")
                # We assume if the table doesn't exist, we return default '1' gap
                try:
                    evidence["critical_coverage_gaps"] = cur.fetchone()['count']
                except Exception:
                    evidence["critical_coverage_gaps"] = 1 # Safety default

                # 4. Go Live
                cur.execute("SELECT verdict FROM atr_go_live_readiness_packages WHERE package_status = 'signed' ORDER BY signed_at DESC LIMIT 1")
                golive = cur.fetchone()
                evidence["go_live_signed"] = golive is not None and golive['verdict'] in ('GO_LIVE', 'GO_LIVE_WITH_CONSTRAINTS')

                # 5. Quarantine
                cur.execute("SELECT count(*) FROM execution_quarantine_ledger WHERE action = 'QUARANTINED'")
                evidence["critical_quarantine_active"] = cur.fetchone()['count'] > 0

                # 6. E2E (Check if coverage audit passed recently)
                cur.execute("SELECT status FROM atr_policy_coverage_inventory ORDER BY created_at DESC LIMIT 1")
                inventory = cur.fetchone()
                evidence["e2e_acceptance_passed"] = inventory is not None and inventory['status'] == 'certified'

                # 7. Stabilization
                stab = self.check_stabilization_window(conn)
                evidence["stabilization_passed"] = stab["passed"]
                evidence["stabilization_details"] = stab

        except Exception as e:
            logger.error(f"Error triaging closure evidence: {e}")

        return evidence

    def evaluate_closure_criteria(
        self,
        charter_active: bool,
        enforcement_map_active: bool,
        critical_coverage_gaps: int,
        e2e_acceptance_passed: bool,
        go_live_signed: bool,
        critical_quarantine_active: bool
    ) -> bool:
        """
        Evaluate if critical closure criteria are met.
        Any failure here prevents PROGRAM_CLOSED.
        """
        if not charter_active:
            logger.warning("Closure criteria failed: Charter not active")
            ATR_PROGRAM_CLOSURE_BLOCK_TOTAL.labels(reason_code="CHARTER_INACTIVE").inc()
            return False
        if not enforcement_map_active:
            logger.warning("Closure criteria failed: Enforcement map not active")
            ATR_PROGRAM_CLOSURE_BLOCK_TOTAL.labels(reason_code="ENFORCEMENT_INACTIVE").inc()
            return False
        if critical_coverage_gaps > 0:
            logger.warning(f"Closure criteria failed: {critical_coverage_gaps} critical coverage gaps open")
            ATR_PROGRAM_CLOSURE_BLOCK_TOTAL.labels(reason_code="CRITICAL_COVERAGE_GAPS").inc()
            return False
        if not e2e_acceptance_passed:
            logger.warning("Closure criteria failed: E2E acceptance not passed")
            ATR_PROGRAM_CLOSURE_BLOCK_TOTAL.labels(reason_code="E2E_NOT_PASSED").inc()
            return False
        if not go_live_signed:
            logger.warning("Closure criteria failed: Go-live readiness not signed")
            ATR_PROGRAM_CLOSURE_BLOCK_TOTAL.labels(reason_code="GO_LIVE_NOT_SIGNED").inc()
            return False
        if critical_quarantine_active:
            logger.warning("Closure criteria failed: Active critical quarantine")
            ATR_PROGRAM_CLOSURE_BLOCK_TOTAL.labels(reason_code="ACTIVE_QUARANTINE").inc()
            return False

        return True

    def build_handoff_matrix(self, package_id: str, handoffs_input: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Build and validate handoff matrix. All 6 domains must be present and accepted.
        """
        matrix = []
        provided_domains = set()

        for h in handoffs_input:
            domain = h.get("domain")
            if not domain:
                continue

            provided_domains.add(domain)
            status = h.get("status", DomainHandoffStatus.PENDING)

            primary_owner = h.get("primary_owner")
            if not primary_owner:
                logger.warning(f"Domain {domain} missing primary_owner")

            matrix.append({
                "handoff_id": f"{package_id}_{domain}",
                "package_id": package_id,
                "domain": domain,
                "primary_owner": primary_owner,
                "secondary_owner": h.get("secondary_owner"),
                "oncall_route": h.get("oncall_route"),
                "status": status,
                "handoff_json": h.get("handoff_json", {})
            })

        missing_domains = set(self.required_domains) - provided_domains
        if missing_domains:
            logger.error(f"Missing required handoff domains: {missing_domains}")
            # we still return what we built, but compute_program_closure_verdict will catch it

        return matrix

    def classify_residual_backlog(self, package_id: str, backlog_input: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Classifies residual backlog items.
        """
        classified = []
        for i, item in enumerate(backlog_input):
            backlog_class = item.get("backlog_class", ResidualBacklogClass.BLOCKING)

            # Simple validation: if it affects protective or execution critically, it shouldn't be non_blocking
            domain = item.get("domain")
            if backlog_class == ResidualBacklogClass.NON_BLOCKING and (item.get("priority") in ["P0"]):
                logger.warning(f"P0 item {item.get('title')} cannot be non-blocking. Elevating to blocking.")
                backlog_class = ResidualBacklogClass.BLOCKING

            classified.append({
                "item_id": item.get("item_id", f"{package_id}_bl_{i}"),
                "package_id": package_id,
                "domain": item.get("domain", "unknown"),
                "priority": item.get("priority", "P1"),
                "status": item.get("status", "open"),
                "backlog_class": backlog_class,
                "title": item.get("title", "Untitled backlog item"),
                "reason_code": item.get("reason_code", "UNKNOWN"),
                "backlog_json": item.get("backlog_json", {})
            })
        return classified

    def compute_program_closure_verdict(
        self,
        criteria_pass: bool,
        handoff_matrix: list[dict[str, Any]],
        classified_backlog: list[dict[str, Any]]
    ) -> ProgramClosureVerdict:
        """
        Calculate the final verdict.
        """
        # 1. Handoff check
        accepted_domains = set()
        for h in handoff_matrix:
            if h["status"] == DomainHandoffStatus.ACCEPTED and h.get("primary_owner") and h.get("oncall_route"):
                accepted_domains.add(h["domain"])

        missing_domains = set(self.required_domains) - accepted_domains
        if missing_domains:
            logger.warning(f"Cannot close due to incomplete handoffs: {missing_domains}")
            return ProgramClosureVerdict.REJECT_CLOSE

        # 2. Criteria check
        if not criteria_pass:
            return ProgramClosureVerdict.REJECT_CLOSE

        # 3. Backlog check
        has_blocking = False
        has_non_blocking = False

        for item in classified_backlog:
            if item["backlog_class"] == ResidualBacklogClass.BLOCKING:
                has_blocking = True
            else:
                has_non_blocking = True

        if has_blocking:
            return ProgramClosureVerdict.HOLD_OPEN

        if has_non_blocking:
            return ProgramClosureVerdict.CLOSED_WITH_RESIDUAL_BACKLOG

        return ProgramClosureVerdict.PROGRAM_CLOSED

    def build_program_closure_package(
        self,
        package_id: str,
        charter_version: str,
        target_scope: str,
        criteria_inputs: dict[str, Any],
        handoffs_input: list[dict[str, Any]],
        backlog_input: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Builds the entire closure package, computes verdict, and creates records.
        """
        criteria_pass = self.evaluate_closure_criteria(
            charter_active=criteria_inputs.get("charter_active", False),
            enforcement_map_active=criteria_inputs.get("enforcement_map_active", False),
            critical_coverage_gaps=criteria_inputs.get("critical_coverage_gaps", 1),
            e2e_acceptance_passed=criteria_inputs.get("e2e_acceptance_passed", False),
            go_live_signed=criteria_inputs.get("go_live_signed", False),
            critical_quarantine_active=criteria_inputs.get("critical_quarantine_active", True),
        )

        handoffs = self.build_handoff_matrix(package_id, handoffs_input)
        backlog = self.classify_residual_backlog(package_id, backlog_input)

        verdict = self.compute_program_closure_verdict(criteria_pass, handoffs, backlog)

        status = ProgramClosureStatus.READY
        if verdict == ProgramClosureVerdict.REJECT_CLOSE:
            status = ProgramClosureStatus.REJECTED

        package = {
            "package_id": package_id,
            "charter_version": charter_version,
            "target_scope": target_scope,
            "status": status,
            "verdict": verdict,
            "summary_json": {
                "criteria_eval": criteria_inputs,
                "handoff_count": len(handoffs),
                "backlog_count": len(backlog)
            },
            "handoffs": handoffs,
            "backlog": backlog
        }

        self._save_package(package)

        # Emit metrics
        ATR_PROGRAM_CLOSURE_PACKAGES_TOTAL.labels(
            status=package["status"],
            verdict=package["verdict"]
        ).inc()

        for h in package["handoffs"]:
            ATR_PROGRAM_HANDOFFS_TOTAL.labels(
                domain=h["domain"],
                status=h["status"]
            ).inc()

        for b in package["backlog"]:
            ATR_PROGRAM_RESIDUAL_BACKLOG_TOTAL.labels(
                domain=b["domain"],
                backlog_class=b["backlog_class"],
                status=b["status"]
            ).inc()

        return package

    def _save_package(self, package: dict[str, Any]):
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:  # type: ignore
                    # Insert package
                    cur.execute(
                        """
                        INSERT INTO atr_program_closure_packages
                        (package_id, charter_version, target_scope, status, verdict, summary_json)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (package_id) DO UPDATE SET
                            status = EXCLUDED.status,
                            verdict = EXCLUDED.verdict,
                            summary_json = EXCLUDED.summary_json
                        """,
                        (
                            package["package_id"],
                            package["charter_version"],
                            package["target_scope"],
                            package["status"],
                            package["verdict"],
                            json.dumps(package["summary_json"])
                        )
                    )

                    # Insert handoffs
                    for h in package["handoffs"]:
                        cur.execute(
                            """
                        INSERT INTO atr_program_handoffs
                            (handoff_id, package_id, domain, primary_owner, secondary_owner, oncall_route, status, handoff_json)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (handoff_id) DO UPDATE SET
                                status = EXCLUDED.status,
                                primary_owner = EXCLUDED.primary_owner,
                                handoff_json = EXCLUDED.handoff_json
                            """,
                            (
                                h["handoff_id"],
                                h["package_id"],
                                h["domain"],
                                h["primary_owner"],
                                h["secondary_owner"],
                                h["oncall_route"],
                                h["status"],
                                json.dumps(h["handoff_json"])
                            )
                        )

                    # Insert backlog
                    for b in package["backlog"]:
                        cur.execute(
                            """
                        INSERT INTO atr_program_residual_backlog
                            (item_id, package_id, domain, priority, status, backlog_class, title, reason_code, backlog_json)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (item_id) DO UPDATE SET
                                status = EXCLUDED.status,
                                backlog_class = EXCLUDED.backlog_class
                            """,
                            (
                                b["item_id"],
                                b["package_id"],
                                b["domain"],
                                b["priority"],
                                b["status"],
                                b["backlog_class"],
                                b["title"],
                                b["reason_code"],
                                json.dumps(b["backlog_json"])
                            )
                        )
        except Exception as e:
            logger.error(f"Failed to save closure package: {e}")
