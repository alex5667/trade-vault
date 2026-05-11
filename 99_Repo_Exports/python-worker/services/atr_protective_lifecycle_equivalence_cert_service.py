from __future__ import annotations

"""
Phase 8.6 — Protective Lifecycle Equivalence Cert Service

Compares legacy protective state with graph protective state and records
equivalence checks, drifts, and cutover readiness.

Checks C1–C7:
    C1  legacy BE state == graph BE state
    C2  legacy trailing state == graph trailing state
    C3  current SL equal (within deterministic tolerance)
    C4  no graph-only TP1 activation mismatch
    C5  no graph-only trailing activation mismatch
    C6  closeout state equals closed_trades truth
    C7  slippage feedback node equals evidence

Readiness ladder:
    not_ready → shadow_healthy → ready_for_read → ready_for_enforce
"""

import json
import logging
import os
import uuid
from datetime import UTC, datetime
from typing import Any

from prometheus_client import Counter

logger = logging.getLogger("atr_protective_equivalence_cert")

# ─── Prometheus metrics ───────────────────────────────────────────────────────

CERT_EQUIVALENCE_TOTAL = Counter(
    "atr_protective_graph_equivalence_total",
    "Protective lifecycle equivalence checks",
    ["status"],
)
CERT_DRIFT_TOTAL = Counter(
    "atr_protective_graph_drift_total",
    "Protective lifecycle drifts detected",
    ["drift_kind", "severity"],
)
CERT_BE_MISMATCH = Counter(
    "atr_protective_be_mismatch_total",
    "Break-even state mismatches",
)
CERT_TRAILING_MISMATCH = Counter(
    "atr_protective_trailing_mismatch_total",
    "Trailing state mismatches",
)
CERT_SL_RATCHET_BACKWARDS = Counter(
    "atr_protective_sl_ratchet_backwards_total",
    "SL ratchet backwards drifts (PAGE)",
)
CERT_CLOSEOUT_MISMATCH = Counter(
    "atr_protective_closeout_mismatch_total",
    "Closeout state mismatches",
)
CERT_CUTOVER_READINESS = Counter(
    "atr_protective_cutover_readiness_total",
    "Cutover readiness evaluations",
    ["status"],
)

# ─── Severity map ─────────────────────────────────────────────────────────────

DRIFT_SEVERITY: dict[str, str] = {
    "be_before_tp1":              "critical",
    "trailing_before_be":         "critical",
    "sl_ratchet_backwards":       "critical",
    "break_even_state_mismatch":  "error",
    "trailing_state_mismatch":    "error",
    "sl_value_mismatch":          "error",
    "tp1_activation_mismatch":    "error",
    "trailing_activation_mismatch": "error",
    "position_state_mismatch":    "error",
    "closeout_reason_mismatch":   "critical",
    "closeout_metrics_mismatch":  "error",
    "slippage_feedback_mismatch": "warn",
    "projection_stale":           "warn",
    "missing_position_node":      "error",
    "missing_closeout_node":      "error",
}

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _gen_id(prefix: str) -> str:
    ts = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}_{uuid.uuid4().hex[:8]}"


# SL tolerance: 0.01% of SL value (to handle floating-point rounding)
_SL_TOLERANCE_PCT = float(os.getenv("ATR_PROTECTIVE_SL_TOLERANCE_PCT", "0.01"))


def _sl_equal(legacy_sl: float, graph_sl: float) -> bool:
    """Check SL equality within tolerance."""
    if legacy_sl == 0 and graph_sl == 0:
        return True
    if legacy_sl == 0 or graph_sl == 0:
        return False
    pct_diff = abs(legacy_sl - graph_sl) / max(abs(legacy_sl), 1e-12) * 100
    return pct_diff <= _SL_TOLERANCE_PCT


# ─── Core cert service ────────────────────────────────────────────────────────

class ATRProtectiveLifecycleEquivalenceCertService:
    """
    Runs C1–C7 checks comparing legacy vs graph protective state
    and records results to SQL.
    """

    @staticmethod
    def run_check(
        signal_id: str,
        legacy_state: dict[str, Any],
        graph_state: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Compare legacy and graph protective states.
        Returns check result with drifts list.
        """
        drifts: list[dict[str, Any]] = []

        # C1: break-even state
        leg_be = legacy_state.get("break_even_state", "unknown")
        grp_be = graph_state.get("break_even_state", "unknown")
        if leg_be != grp_be:
            drifts.append({
                "drift_kind": "break_even_state_mismatch",
                "severity": DRIFT_SEVERITY["break_even_state_mismatch"],
                "reason_code": f"legacy={leg_be}_graph={grp_be}",
                "drift_json": {"legacy": leg_be, "graph": grp_be},
            })
            CERT_BE_MISMATCH.inc()

        # C2: trailing state
        leg_trail = legacy_state.get("trailing_state", "unknown")
        grp_trail = graph_state.get("trailing_state", "unknown")
        if leg_trail != grp_trail:
            drifts.append({
                "drift_kind": "trailing_state_mismatch",
                "severity": DRIFT_SEVERITY["trailing_state_mismatch"],
                "reason_code": f"legacy={leg_trail}_graph={grp_trail}",
                "drift_json": {"legacy": leg_trail, "graph": grp_trail},
            })
            CERT_TRAILING_MISMATCH.inc()

        # C3: current SL
        leg_sl = float(legacy_state.get("current_sl") or 0)
        grp_sl = float(graph_state.get("current_sl") or 0)
        if leg_sl > 0 and grp_sl > 0 and not _sl_equal(leg_sl, grp_sl):
            drifts.append({
                "drift_kind": "sl_value_mismatch",
                "severity": DRIFT_SEVERITY["sl_value_mismatch"],
                "reason_code": f"legacy_sl={leg_sl}_graph_sl={grp_sl}",
                "drift_json": {"legacy_sl": leg_sl, "graph_sl": grp_sl},
            })

        # C4: TP1 activation mismatch
        leg_tp1 = legacy_state.get("tp1_reached", False)
        grp_tp1 = graph_state.get("tp1_reached", False)
        if leg_tp1 != grp_tp1:
            drifts.append({
                "drift_kind": "tp1_activation_mismatch",
                "severity": DRIFT_SEVERITY["tp1_activation_mismatch"],
                "reason_code": f"legacy_tp1={leg_tp1}_graph_tp1={grp_tp1}",
                "drift_json": {"legacy": leg_tp1, "graph": grp_tp1},
            })

        # C5: trailing activation mismatch
        leg_trail_active = leg_trail in ("active", "armed")
        grp_trail_active = grp_trail in ("active", "armed")
        if leg_trail_active != grp_trail_active:
            drifts.append({
                "drift_kind": "trailing_activation_mismatch",
                "severity": DRIFT_SEVERITY["trailing_activation_mismatch"],
                "reason_code": f"legacy_active={leg_trail_active}_graph_active={grp_trail_active}",
                "drift_json": {"legacy": leg_trail, "graph": grp_trail},
            })

        # C6: closeout match (when both closed)
        leg_closeout = legacy_state.get("closeout_state")
        grp_closeout = graph_state.get("closeout_state")
        if leg_closeout and grp_closeout:
            if leg_closeout.get("close_reason") != grp_closeout.get("close_reason"):
                drifts.append({
                    "drift_kind": "closeout_reason_mismatch",
                    "severity": DRIFT_SEVERITY["closeout_reason_mismatch"],
                    "reason_code": "close_reason_differs",
                    "drift_json": {
                        "legacy_reason": leg_closeout.get("close_reason"),
                        "graph_reason": grp_closeout.get("close_reason"),
                    },
                })
                CERT_CLOSEOUT_MISMATCH.inc()

            # Exit price / PnL tolerance check
            leg_exit = float(leg_closeout.get("exit_price", 0) or 0)
            grp_exit = float(grp_closeout.get("exit_price", 0) or 0)
            if leg_exit > 0 and grp_exit > 0:
                if not _sl_equal(leg_exit, grp_exit):
                    drifts.append({
                        "drift_kind": "closeout_metrics_mismatch",
                        "severity": DRIFT_SEVERITY["closeout_metrics_mismatch"],
                        "reason_code": "exit_price_differs",
                        "drift_json": {
                            "legacy_exit": leg_exit,
                            "graph_exit": grp_exit,
                        },
                    })
        elif leg_closeout and not grp_closeout:
            drifts.append({
                "drift_kind": "missing_closeout_node",
                "severity": DRIFT_SEVERITY["missing_closeout_node"],
                "reason_code": "legacy_closed_but_graph_has_no_closeout",
                "drift_json": {"legacy_closeout": leg_closeout},
            })

        # C7: slippage feedback match
        leg_slip = legacy_state.get("slippage_feedback")
        grp_slip = graph_state.get("slippage_feedback")
        if leg_slip and grp_slip:
            leg_bps = float(leg_slip.get("slippage_bps", 0) or 0)
            grp_bps = float(grp_slip.get("slippage_bps", 0) or 0)
            if abs(leg_bps - grp_bps) > 0.5:
                drifts.append({
                    "drift_kind": "slippage_feedback_mismatch",
                    "severity": DRIFT_SEVERITY["slippage_feedback_mismatch"],
                    "reason_code": f"legacy_bps={leg_bps}_graph_bps={grp_bps}",
                    "drift_json": {"legacy_bps": leg_bps, "graph_bps": grp_bps},
                })

        # Increment drift counters
        for d in drifts:
            CERT_DRIFT_TOTAL.labels(
                drift_kind=d["drift_kind"], severity=d["severity"],
            ).inc()

        status = "passed" if not drifts else "failed"
        CERT_EQUIVALENCE_TOTAL.labels(status=status).inc()

        # Phase 10.2: Charter Enforcement Map (L8)
        enforcement = {"overall_action": "allow"}
        if critical:  # type: ignore
            try:
                from services.atr_charter_compliance_engine import ATRCharterComplianceEngine
                engine = ATRCharterComplianceEngine()
                # Use signal_id as context_ref for protective path
                bundle = engine.evaluate_context(
                    context_kind="protective_lifecycle_context",
                    context_ref=signal_id
                )
                enforcement = bundle.get("enforcement", {"overall_action": "allow"})

                if enforcement["overall_action"] != "allow":
                    logger.warning(
                        "🚨 ENFORCEMENT TRIGGERED for %s: %s",
                        signal_id, enforcement["overall_action"]
                    )
            except Exception as ce:
                logger.error("Phase 10.2 protective enforcement check failed: %s", ce)

        return {
            "signal_id": signal_id,
            "status": status,
            "drifts": drifts,
            "critical_count": len(critical),  # type: ignore
            "enforcement": enforcement,
            "legacy_state": legacy_state,
            "graph_state": graph_state,
        }

    @staticmethod
    def persist_check(check_result: dict[str, Any]) -> str | None:
        """Persist equivalence check and any drifts to SQL."""
        try:
            from services.analytics_db import get_conn

            check_id = _gen_id("pchk")
            signal_id = check_result["signal_id"]
            status = check_result["status"]
            drifts = check_result.get("drifts", [])
            summary = {
                "drift_count": len(drifts),
                "critical_count": check_result.get("critical_count", 0),
                "matching": status == "passed",
            }

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO atr_protective_equivalence_checks (
                            check_id, signal_id, legacy_state_json,
                            graph_state_json, status, summary_json
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        check_id, signal_id,
                        json.dumps(check_result.get("legacy_state", {})),
                        json.dumps(check_result.get("graph_state", {})),
                        status, json.dumps(summary),
                    ))

                    for d in drifts:
                        drift_id = _gen_id("pdrift")
                        cur.execute("""
                            INSERT INTO atr_protective_drifts (
                                drift_id, signal_id, drift_kind, severity,
                                status, reason_code, drift_json
                            ) VALUES (%s, %s, %s, %s, 'open', %s, %s)
                        """, (
                            drift_id, signal_id,
                            d["drift_kind"], d["severity"],
                            d["reason_code"], json.dumps(d.get("drift_json", {})),
                        ))
                conn.commit()

            return check_id
        except Exception as exc:
            logger.error("persist_check failed for %s: %s", check_result.get("signal_id"), exc)
            return None

    @staticmethod
    def evaluate_cutover_readiness(
        component: str = "protective_lifecycle",
    ) -> tuple[str, dict[str, Any]]:
        """
        Evaluate cutover readiness ladder.

        shadow_healthy:   7d no critical drift, 100% match on bounded scopes
        ready_for_read:   shadow_healthy + 3 more days
        ready_for_enforce: ready_for_read + 14d 100% match
        """
        try:
            import psycopg2.extras

            from services.analytics_db import get_conn

            with get_conn() as conn, conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                # Critical drifts in last 7 days
                cur.execute("""
                    SELECT count(*) AS c
                    FROM atr_protective_drifts
                    WHERE severity = 'critical'
                      AND status = 'open'
                      AND created_at > now() - interval '7 days'
                """)
                critical_7d = cur.fetchone()["c"]  # type: ignore

                # Match rate in last 7 days
                cur.execute("""
                    SELECT
                        count(*) FILTER (WHERE status = 'passed') AS passed,
                        count(*) AS total
                    FROM atr_protective_equivalence_checks
                    WHERE created_at > now() - interval '7 days'
                """)
                row = cur.fetchone()
                total = row["total"]  # type: ignore
                passed = row["passed"]  # type: ignore
                pct_match = (passed / total * 100) if total > 0 else 0.0

                # Days without critical drift
                cur.execute("""
                    SELECT MAX(created_at) AS last_critical
                    FROM atr_protective_drifts
                    WHERE severity = 'critical'
                """)
                last_crit = cur.fetchone()["last_critical"]  # type: ignore
                if last_crit:
                    days_without = (
                        datetime.now(tz=UTC) - last_crit.replace(tzinfo=UTC)
                    ).days
                else:
                    days_without = 14  # No critical ever = good

                summary = {
                    "critical_drifts_7d": critical_7d,
                    "pct_match": round(pct_match, 2),
                    "total_checks_7d": total,
                    "days_without_critical": days_without,
                    "evaluated_at": datetime.now(tz=UTC).isoformat(),
                }

                # Ladder logic
                if critical_7d == 0 and pct_match >= 100.0:
                    if days_without >= 14:
                        # Check previous status
                        cur.execute("""
                            SELECT status FROM atr_protective_cutover_readiness
                            WHERE component = %s
                            ORDER BY created_at DESC LIMIT 1
                        """, (component,))
                        prev = cur.fetchone()
                        prev_status = prev["status"] if prev else "not_ready"
                        if prev_status in ("ready_for_read", "ready_for_enforce"):
                            new_status = "ready_for_enforce"
                        else:
                            new_status = "ready_for_read"
                    elif days_without >= 7:
                        new_status = "shadow_healthy"
                    else:
                        new_status = "not_ready"
                else:
                    new_status = "not_ready"

                # Persist readiness
                readiness_id = _gen_id("prdy")
                with conn.cursor() as cur2:
                    cur2.execute("""
                        INSERT INTO atr_protective_cutover_readiness (
                            readiness_id, component, status, summary_json
                        ) VALUES (%s, %s, %s, %s)
                    """, (readiness_id, component, new_status, json.dumps(summary)))
                conn.commit()

                CERT_CUTOVER_READINESS.labels(status=new_status).inc()
                logger.info(
                    "Protective cutover readiness: %s (7d_critical=%d, pct=%.1f%%)",
                    new_status, critical_7d, pct_match,
                )
                return new_status, summary
        except Exception as exc:
            logger.error("evaluate_cutover_readiness failed: %s", exc, exc_info=True)
            return "not_ready", {"error": str(exc)}


# ─── Telegram message builders ────────────────────────────────────────────────

def render_protective_shadow_healthy(
    positions_checked: int,
    critical_drifts: int,
) -> str:
    icon = "✅" if critical_drifts == 0 else "⚠️"
    return (
        f"{icon} <b>ATR Graph Protective Lifecycle Shadow</b>\n\n"
        f"Component: <code>protective_lifecycle</code>\n"
        f"Status: <code>SHADOW_HEALTHY</code>\n"
        f"Positions checked: <code>{positions_checked}</code>\n"
        f"Critical drifts: <code>{critical_drifts}</code>"
    )


def render_protective_critical_drift(
    signal_id: str,
    symbol: str,
    drift_kind: str,
    legacy_val: str,
    graph_val: str,
    severity: str,
) -> str:
    return (
        f"🚨 <b>ATR Graph Protective Lifecycle Drift</b>\n\n"
        f"Signal: <code>{signal_id}</code>\n"
        f"Symbol: <code>{symbol}</code>\n"
        f"Drift: <code>{drift_kind}</code>\n"
        f"Legacy: <code>{legacy_val}</code>\n"
        f"Graph: <code>{graph_val}</code>\n"
        f"Severity: <b>{severity.upper()}</b>"
    )


def render_protective_cutover_ready(
    status: str,
    summary: dict[str, Any],
) -> str:
    icon = {
        "not_ready": "🔴",
        "shadow_healthy": "🟡",
        "ready_for_read": "🔵",
        "ready_for_enforce": "🟢",
    }.get(status, "⚪")
    lines = [
        f"{icon} <b>ATR Graph Protective Lifecycle Cutover</b>",
        "",
        "Component: <code>protective_lifecycle</code>",
        f"Status: <code>{status.upper()}</code>",
        "Evidence:",
        f"  • 7d critical drifts: <code>{summary.get('critical_drifts_7d', '?')}</code>",
        f"  • State match: <code>{summary.get('pct_match', 0):.0f}%</code>",
        f"  • Days w/o critical: <code>{summary.get('days_without_critical', '?')}</code>",
    ]
    return "\n".join(lines)
