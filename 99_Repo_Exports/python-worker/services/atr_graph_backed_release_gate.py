from __future__ import annotations

"""
Phase 8.2 — Graph-backed release gate (atr_graph_backed_release_gate.py)

Cutover modes (controlled via ATR_GRAPH_RELEASE_GATE_MODE env):
    shadow_compare       — graph builds parallel decision, compares vs legacy, no enforcement
    graph_read_primary   — graph decision is primary for UI/auditor/Telegram; legacy = enforcement fallback
    graph_enforced       — graph decision IS enforcement truth

Enable flag:
    ATR_GRAPH_RELEASE_GATE_ENABLE = "1" (default "0" — off until shadow_healthy)

Bounded scopes for Phase 8.2:
    BTCUSDT, ETHUSDT  ×  stop_ttl layer  ×  canary_25 / live_100 targets
"""

import json
import logging
import os
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from services.analytics_db import get_conn

logger = logging.getLogger("atr_graph_release_gate")

# ─── ENV ──────────────────────────────────────────────────────────────────────

_ENABLE      = os.getenv("ATR_GRAPH_RELEASE_GATE_ENABLE", "0") == "1"
_MODE        = os.getenv("ATR_GRAPH_RELEASE_GATE_MODE", "shadow_compare")
# shadow_compare | graph_read_primary | graph_enforced

# Bounded scopes for Phase 8.2
_BOUNDED_SYMBOLS = {"BTCUSDT", "ETHUSDT"}
_BOUNDED_LAYERS  = {"stop_ttl"}
_BOUNDED_STAGES  = {"canary_25", "live_100"}

# Drift severity map
_DRIFT_SEVERITY: dict[str, str] = {
    "release_decision_mismatch":   "critical",
    "missing_replay_cert_edge":    "critical",   # becomes critical when scope targets live_100
    "missing_rollout_cert_edge":   "error",
    "missing_freeze_blocker":      "error",
    "missing_override_constraint": "warn",
    "readiness_score_mismatch":    "warn",
    "blocker_set_mismatch":        "error",
    "warning_set_mismatch":        "warn",
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _gen_id(prefix: str) -> str:
    ts = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}_{uuid.uuid4().hex[:6]}"


def _is_bounded_scope(symbol: str | None, layer: str | None, stage: str | None) -> bool:
    """Returns True if this scope is within the Phase 8.2 bounded pilot."""
    symbol_ok = symbol in _BOUNDED_SYMBOLS if symbol else False
    layer_ok  = (layer in _BOUNDED_LAYERS)  if layer  else True   # None = don't filter
    stage_ok  = (stage in _BOUNDED_STAGES)  if stage  else True
    return symbol_ok and layer_ok and stage_ok


# ─── Graph effective release state ────────────────────────────────────────────

def build_graph_release_state(change_id: str) -> dict[str, Any] | None:
    """
    Load the graph-derived effective release snapshot for a given change_id.
    Returns None if graph doesn't have enough data (missing nodes).

    Uses v_control_plane_effective_release_state for resolved projection,
    plus supplementary checks (SEV-1 incidents, overdue actions, error budget).
    """
    try:
        with get_conn() as conn, conn.cursor(
            cursor_factory=__import__("psycopg2").extras.RealDictCursor
        ) as cur:
            # 1. Fetch Change Request
            cur.execute(
                "SELECT * FROM atr_change_requests WHERE change_id = %s", (change_id,)
            )
            change = cur.fetchone()
            if not change:
                logger.warning("Graph release state: change_id %s not found", change_id)
                return None

            symbol    = change.get("symbol")
            layer     = change.get("layer")
            scenario  = change.get("scenario", "")
            risk_lvl  = change.get("risk_level", "medium")
            scope_val = symbol or "global"

            # 2. Resolved graph projection (via view)
            cur.execute(
                """
                SELECT *
                FROM v_control_plane_effective_release_state
                WHERE scope_value = %s
                LIMIT 1
                """
                (scope_val,),
            )
            proj = cur.fetchone()

            # 3. Guard against cold-start / missing graph data (A6)
            if not proj:
                return None

            rollout_stage      = proj.get("rollout_stage") or "none"
            release_decision   = proj.get("release_decision") or "deny"
            freeze_state       = proj.get("freeze_state") or "none"
            override_state     = proj.get("override_state") or "none"
            replay_cert_status = proj.get("replay_cert_status") or "missing"
            rollout_cert_status= proj.get("rollout_cert_status") or "missing"

            # 4. Supplementary: open SEV-1 incidents
            cur.execute(
                "SELECT count(*) AS c FROM atr_incidents WHERE status != 'closed' AND severity = 'SEV-1'"
            )
            sev1_open = cur.fetchone()["c"]

            # 5. Supplementary: overdue corrective actions
            now_ms = int(time.time() * 1000)
            cur.execute(
                """
                SELECT count(*) AS c FROM atr_corrective_actions
                WHERE status NOT IN ('done', 'verified', 'dropped') AND due_at_ms < %s
                """
                (now_ms,),
            )
            overdue_actions = cur.fetchone()["c"]

            # 6. Error budget
            cur.execute(
                """
                SELECT budget_status
                FROM atr_invariant_budget_states
                WHERE scope_value IN (%s, %s, %s)
                  AND budget_status IN ('exhausted', 'warning')
                """
                (symbol, layer, (change.get("policy_ver", ""))),
            )
            budget_rows     = cur.fetchall()
            budget_exhausted = any(r["budget_status"] == "exhausted" for r in budget_rows)
            budget_warning   = any(r["budget_status"] == "warning"   for r in budget_rows)

            # 7. Build blockers / warnings from graph state
            blockers: list[str] = []
            warnings: list[str] = []

            if freeze_state not in ("none", None, ""):
                blockers.append("active_freeze_on_scope")

            if sev1_open > 0:
                blockers.append("INV_NO_LIVE_SCOPE_WITH_OPEN_CRITICAL_INCIDENT")

            is_live_or_canary = any(str(scenario).startswith(p) for p in ("live", "canary"))

            if replay_cert_status != "passed" and is_live_or_canary:
                blockers.append("missing_replay_cert_edge")

            if rollout_cert_status != "passed" and is_live_or_canary:
                if risk_lvl in ("high", "critical"):
                    blockers.append("INV_NO_STAGE_ADVANCE_WITHOUT_ROLLOUT_CERT")
                else:
                    warnings.append("low_sample_rollout_stage")

            if overdue_actions > 0 and risk_lvl in ("high", "critical"):
                blockers.append("INV_NO_OVERRIDE_RELEASE_WITH_UNRESOLVED_CRITICAL_POSTMORTEM_ACTION")
            elif overdue_actions > 0:
                warnings.append("medium_overdue_action")

            if budget_exhausted:
                blockers.append("INVARIANT_ERROR_BUDGET_EXHAUSTED")
            elif budget_warning:
                warnings.append("INVARIANT_ERROR_BUDGET_WARNING")

            # 8. Unresolved critical invariant violations
            cur.execute(
                """
                SELECT count(*) AS c
                FROM atr_invariant_violations v
                JOIN atr_invariants i ON v.invariant_id = i.invariant_id
                WHERE v.status != 'resolved' AND i.enforcement_mode = 'release_block'
                """
            )
            unresolved_invs = cur.fetchone()["c"]
            if unresolved_invs > 0:
                blockers.append("INV_UNRESOLVED_CRITICAL_INVARIANTS_ON_SCOPE")

            # 9. Derive graph decision
            if blockers:
                graph_decision = "deny"
            elif warnings:
                graph_decision = "allow_with_override"
            else:
                graph_decision = "allow"

            # Scorecard readiness score (simple mirror of legacy logic)
            score = 35.0
            if replay_cert_status == "passed":  score += 20.0
            if rollout_cert_status == "passed": score += 20.0
            if sev1_open == 0:                  score += 15.0
            if overdue_actions == 0:            score += 10.0
            if "low_sample_rollout_stage" in warnings:  score -= 6.0
            if "medium_overdue_action" in warnings:      score -= 5.0
            score = max(0.0, min(100.0, score))

            return {
                "scope": {
                    "source":              change.get("source"),
                    "venue":               change.get("venue", "binance_futures"),
                    "symbol":              symbol,
                    "scenario":            scenario,
                    "regime":              change.get("regime"),
                    "risk_horizon_bucket": change.get("risk_horizon_bucket"),
                    "layer":               layer,
                    "policy_ver":          change.get("policy_ver"),
                },
                "release_state": {
                    "target_stage":                 rollout_stage,
                    "replay_cert_status":           replay_cert_status,
                    "required_rollout_cert_status": rollout_cert_status,
                    "active_freeze_state":          freeze_state or "none",
                    "open_related_sev1_incidents":  sev1_open,
                    "overdue_p0_p1_actions":        overdue_actions,
                    "override_state":               override_state or "none",
                    "invariant_budget_status":      "exhausted" if budget_exhausted else (
                        "warning" if budget_warning else "healthy"
                    ),
                },
                "decision":        graph_decision,
                "readiness_score": score,
                "blockers":        blockers,
                "warnings":        warnings,
                "change_id":       change_id,
                "scope_value":     scope_val,
            }
    except Exception as exc:
        logger.error("build_graph_release_state(%s) failed: %s", change_id, exc, exc_info=True)
        return None


# ─── Dual-read compare ────────────────────────────────────────────────────────

def compare_with_legacy(
    change_id: str,
    legacy_scorecard: dict[str, Any],
    graph_state: dict[str, Any],
) -> dict[str, Any]:
    """
    Compare legacy release scorecard vs graph-derived decision.
    Returns a drift summary dict (ready for DB insertion).
    """
    drifts: list[dict[str, Any]] = []
    scope_val = graph_state.get("scope_value", "unknown")

    legacy_dec = legacy_scorecard.get("decision", "unknown")
    graph_dec  = graph_state.get("decision", "unknown")

    # E1 — decision match
    if legacy_dec != graph_dec:
        drifts.append({
            "drift_kind":  "release_decision_mismatch",
            "severity":    "critical",
            "reason_code": f"legacy={legacy_dec} graph={graph_dec}",
            "drift_json":  {
                "legacy_decision": legacy_dec,
                "graph_decision":  graph_dec,
            },
        })

    # E2 — blockers set
    legacy_blockers = set(legacy_scorecard.get("blockers", []))
    graph_blockers  = set(graph_state.get("blockers", []))
    if legacy_blockers != graph_blockers:
        drifts.append({
            "drift_kind":  "blocker_set_mismatch",
            "severity":    "error",
            "reason_code": "blocker_sets_differ",
            "drift_json":  {
                "only_in_legacy": list(legacy_blockers - graph_blockers),
                "only_in_graph":  list(graph_blockers  - legacy_blockers),
            },
        })

    # E3 — warnings set
    legacy_warnings = set(legacy_scorecard.get("warnings", []))
    graph_warnings  = set(graph_state.get("warnings", []))
    if legacy_warnings != graph_warnings:
        drifts.append({
            "drift_kind":  "warning_set_mismatch",
            "severity":    "warn",
            "reason_code": "warning_sets_differ",
            "drift_json":  {
                "only_in_legacy": list(legacy_warnings - graph_warnings),
                "only_in_graph":  list(graph_warnings  - legacy_warnings),
            },
        })

    # E4 — replay cert edge present in graph
    rel_state = graph_state.get("release_state", {})
    stage = rel_state.get("target_stage", "")
    if stage in _BOUNDED_STAGES and rel_state.get("replay_cert_status") not in ("passed",):
        drifts.append({
            "drift_kind":  "missing_replay_cert_edge",
            "severity":    "critical" if stage == "live_100" else "error",
            "reason_code": f"replay_cert_missing_for_{stage}",
            "drift_json":  {"replay_cert_status": rel_state.get("replay_cert_status")},
        })

    # E5 — missing freeze blocker when scope is frozen
    freeze = rel_state.get("active_freeze_state", "none")
    if freeze not in ("none", None, "") and "active_freeze_on_scope" not in graph_blockers:
        drifts.append({
            "drift_kind":  "missing_freeze_blocker",
            "severity":    "error",
            "reason_code": f"freeze={freeze}_not_in_blockers",
            "drift_json":  {"freeze_state": freeze},
        })

    # E6 — readiness score bucket mismatch (only flag if it changes allow/deny)
    legacy_score = legacy_scorecard.get("readiness_score", 0.0)
    graph_score  = graph_state.get("readiness_score", 0.0)
    if abs(legacy_score - graph_score) > 20.0:
        severity = "error" if legacy_dec != graph_dec else "warn"
        drifts.append({
            "drift_kind":  "readiness_score_mismatch",
            "severity":    severity,
            "reason_code": f"legacy={legacy_score:.1f} graph={graph_score:.1f}",
            "drift_json":  {"legacy_score": legacy_score, "graph_score": graph_score},
        })

    matching = len(drifts) == 0
    return {
        "change_id":      change_id,
        "scope_value":    scope_val,
        "legacy_decision": legacy_dec,
        "graph_decision":  graph_dec,
        "matching":       matching,
        "drifts":         drifts,
    }


# ─── Persistence helpers ──────────────────────────────────────────────────────

def _persist_equivalence_check(
    conn,
    compare_result: dict[str, Any],
) -> str:
    """Persist an equivalence check row and return check_id."""
    check_id = _gen_id("req")
    matching = compare_result["matching"]
    drifts   = compare_result["drifts"]
    critical = [d for d in drifts if d["severity"] == "critical"]
    status   = "passed" if matching else "failed"
    summary  = {
        "drift_count":          len(drifts),
        "critical_drift_count": len(critical),
        "matching":             matching,
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO atr_release_equivalence_checks
                (check_id, change_id, scope_value, legacy_decision, graph_decision, status, summary_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            (
                check_id,
                compare_result["change_id"],
                compare_result["scope_value"],
                compare_result["legacy_decision"],
                compare_result["graph_decision"],
                status,
                json.dumps(summary),
            ),
        )
    return check_id


def _persist_drifts(
    conn,
    change_id: str,
    scope_value: str,
    drifts: list[dict[str, Any]],
) -> None:
    """Persist each drift row with open status."""
    with conn.cursor() as cur:
        for d in drifts:
            drift_id = _gen_id("rdrift")
            cur.execute(
                """
                INSERT INTO atr_release_drifts
                    (drift_id, change_id, scope_value, drift_kind, severity, status, reason_code, drift_json)
                VALUES (%s, %s, %s, %s, %s, 'open', %s, %s)
                """
                (
                    drift_id,
                    change_id,
                    scope_value,
                    d["drift_kind"],
                    d["severity"],
                    d["reason_code"],
                    json.dumps(d.get("drift_json", {})),
                ),
            )


# ─── Main entry-point ─────────────────────────────────────────────────────────

def evaluate_release(
    change_id: str,
    legacy_scorecard: dict[str, Any],
) -> dict[str, Any]:
    """
    Phase 8.2 release gate evaluator.
    Always returns the effective decision dict plus comparison metadata.

    Called by atr_release_gate_service after build_scorecard().
    When _ENABLE=False, returns legacy_scorecard transparently.

    Modes:
        shadow_compare     → compare, persist, log; return legacy decision
        graph_read_primary → compare, persist; return graph state to UI/auditor;
                              legacy enforces hard-deny only
        graph_enforced     → graph is truth; legacy ignored
    """
    if not _ENABLE:
        return {
            "decision":     legacy_scorecard.get("decision", "allow"),
            "source":       "legacy",
            "scorecard":    legacy_scorecard,
            "graph_state":  None,
            "compare":      None,
        }

    symbol   = legacy_scorecard.get("scope", {}).get("symbol")
    layer    = legacy_scorecard.get("scope", {}).get("layer")
    scenario = legacy_scorecard.get("scope", {}).get("scenario", "")
    stage    = (
        legacy_scorecard.get("summary", {}).get("rollout_cert_status", "")
        .replace("_passed", "").replace("_failed", "").replace("_missing", "")
    )
    if not _is_bounded_scope(symbol, layer, stage):
        # out-of-pilot scope: pass through legacy
        return {
            "decision":     legacy_scorecard.get("decision", "allow"),
            "source":       "legacy_out_of_pilot",
            "scorecard":    legacy_scorecard,
            "graph_state":  None,
            "compare":      None,
        }

    graph_state = build_graph_release_state(change_id)
    if graph_state is None:
        logger.warning("Graph state unavailable for %s; falling back to legacy", change_id)
        return {
            "decision":   legacy_scorecard.get("decision", "allow"),
            "source":     "legacy_graph_unavailable",
            "scorecard":  legacy_scorecard,
            "graph_state": None,
            "compare":    None,
        }

    compare  = compare_with_legacy(change_id, legacy_scorecard, graph_state)
    critical = [d for d in compare["drifts"] if d["severity"] == "critical"]

    # Persist compare + drifts
    try:
        with get_conn() as conn:
            check_id = _persist_equivalence_check(conn, compare)
            if compare["drifts"]:
                _persist_drifts(
                    conn,
                    change_id,
                    compare["scope_value"],
                    compare["drifts"],
                )
            conn.commit()
    except Exception as exc:
        logger.error("Failed to persist release equivalence check for %s: %s", change_id, exc)
        check_id = None

    # Mode branching
    if _MODE == "graph_enforced":
        effective_decision = graph_state["decision"]
        source = "graph"
    elif _MODE == "graph_read_primary":
        # Hard-deny from legacy still wins
        if legacy_scorecard.get("decision") == "deny" and graph_state["decision"] != "deny":
            effective_decision = "deny"
            source = "legacy_hard_deny_override_graph"
        else:
            effective_decision = graph_state["decision"]
            source = "graph_primary"
    else:
        # shadow_compare: legacy wins always
        effective_decision = legacy_scorecard.get("decision", "allow")
        source = "legacy_shadow_only"

    if critical:
        logger.warning(
            "CRITICAL release drift on %s (%s): %s",
            change_id,
            compare["scope_value"],
            [d["drift_kind"] for d in critical],
        )

    return {
        "decision":    effective_decision,
        "source":      source,
        "scorecard":   legacy_scorecard,
        "graph_state": graph_state,
        "compare":     compare,
        "check_id":    check_id,
        "critical_drifts": critical,
    }


# ─── Cutover readiness evaluator ─────────────────────────────────────────────

def mark_cutover_readiness(component: str = "release_gate") -> dict[str, Any]:
    """
    Evaluates cutover readiness for the release gate component.
    Status ladder:
        not_ready  →  shadow_healthy  →  ready_for_read  →  ready_for_enforce

    Conditions for shadow_healthy:
        - 7 consecutive days with no critical drift on bounded scopes
        - 100% decision match (allow/deny) on bounded checks
        - cert chain complete (no missing replay edge on live_100 targets)

    Conditions for ready_for_read:
        - shadow_healthy for 3 more days

    Conditions for ready_for_enforce:
        - ready_for_read + 100% match last 14d
    """
    now = datetime.now(tz=UTC)
    try:
        with get_conn() as conn, conn.cursor(
            cursor_factory=__import__("psycopg2").extras.RealDictCursor
        ) as cur:
            # Count critical drifts in last 7 days for bounded scopes
            cur.execute(
                """
                SELECT count(*) AS c
                FROM atr_release_drifts
                WHERE severity = 'critical'
                  AND created_at > now() - interval '7 days'
                  AND status = 'open'
                """
            )
            critical_7d = cur.fetchone()["c"]

            # Total bounded checks in last 7 days
            cur.execute(
                """
                SELECT
                    count(*) FILTER (WHERE status = 'passed') AS passed,
                    count(*)                                  AS total
                FROM atr_release_equivalence_checks
                WHERE created_at > now() - interval '7 days'
                """
            )
            row = cur.fetchone()
            total_checks  = row["total"]
            passed_checks = row["passed"]
            pct_match = (passed_checks / total_checks * 100) if total_checks > 0 else 0.0

            # Missing replay edge critical drifts on live_100
            cur.execute(
                """
                SELECT count(*) AS c
                FROM atr_release_drifts
                WHERE drift_kind = 'missing_replay_cert_edge'
                  AND severity = 'critical'
                  AND status = 'open'
                """
            )
            missing_replay = cur.fetchone()["c"]

            summary = {
                "critical_drifts_7d": critical_7d,
                "pct_decision_match":  round(pct_match, 2),
                "total_checks_7d":     total_checks,
                "missing_replay_cert_edge_live_critical": missing_replay,
                "evaluated_at": now.isoformat(),
            }

            if critical_7d == 0 and pct_match >= 100.0 and missing_replay == 0:
                # Check for 14d window (ready_for_enforce)
                cur.execute(
                    """
                    SELECT count(*) AS c
                    FROM atr_release_drifts
                    WHERE severity = 'critical'
                      AND created_at > now() - interval '14 days'
                      AND status = 'open'
                    """
                )
                critical_14d = cur.fetchone()["c"]

                cur.execute(
                    """
                    SELECT
                        count(*) FILTER (WHERE status = 'passed') AS passed,
                        count(*)                                  AS total
                    FROM atr_release_equivalence_checks
                    WHERE created_at > now() - interval '14 days'
                    """
                )
                row14 = cur.fetchone()
                pct_14d = (row14["passed"] / row14["total"] * 100) if row14["total"] > 0 else 0.0

                if critical_14d == 0 and pct_14d >= 100.0:
                    # Check if already at ready_for_read
                    cur.execute(
                        """
                        SELECT status FROM atr_release_cutover_readiness
                        WHERE component = %s
                        ORDER BY created_at DESC LIMIT 1
                        """
                        (component,),
                    )
                    prev = cur.fetchone()
                    prev_status = prev["status"] if prev else "not_ready"
                    new_status = (
                        "ready_for_enforce"
                        if prev_status in ("ready_for_read", "ready_for_enforce")
                        else "ready_for_read"
                    )
                else:
                    new_status = "shadow_healthy"
            else:
                new_status = "not_ready"

            readiness_id = _gen_id("rdy")
            with conn.cursor() as cur2:
                cur2.execute(
                    """
                    INSERT INTO atr_release_cutover_readiness
                        (readiness_id, component, status, summary_json)
                    VALUES (%s, %s, %s, %s)
                    """
                    (readiness_id, component, new_status, json.dumps(summary)),
                )
            conn.commit()

            logger.info(
                "Cutover readiness for %s: %s (7d_critical=%d, pct_match=%.1f%%)",
                component, new_status, critical_7d, pct_match,
            )
            return {"status": new_status, "summary": summary}

    except Exception as exc:
        logger.error("mark_cutover_readiness(%s) failed: %s", component, exc, exc_info=True)
        return {"status": "not_ready", "error": str(exc)}


# ─── Telegram message builders ────────────────────────────────────────────────

def render_shadow_compare_healthy(scope_count: int, mismatch_count: int, critical_count: int) -> str:
    status_icon = "✅" if mismatch_count == 0 else "⚠️"
    return (
        f"{status_icon} <b>ATR Graph Release Gate Shadow</b>\n\n"
        f"Component: <code>release_gate</code>\n"
        f"Status: <code>SHADOW_HEALTHY</code>\n"
        f"Scopes checked: <code>{scope_count}</code>\n"
        f"Decision mismatches: <code>{mismatch_count}</code>\n"
        f"Critical drifts: <code>{critical_count}</code>"
    )


def render_critical_drift(drift: dict[str, Any], change_id: str, scope_value: str) -> str:
    return (
        f"🚨 <b>ATR Graph Release Drift</b>\n\n"
        f"Change: <code>{change_id}</code>\n"
        f"Scope: <code>{scope_value}</code>\n"
        f"Drift: <code>{drift.get('drift_kind')}</code>\n"
        f"Legacy: <code>{drift.get('drift_json', {}).get('legacy_decision', '?')}</code>\n"
        f"Graph: <code>{drift.get('drift_json', {}).get('graph_decision', '?')}</code>\n"
        f"Severity: <b>{drift.get('severity', '?').upper()}</b>"
    )


def render_cutover_ready(status: str, summary: dict[str, Any]) -> str:
    icon = {"not_ready": "🔴", "shadow_healthy": "🟡", "ready_for_read": "🔵", "ready_for_enforce": "🟢"}.get(status, "⚪")
    lines = [
        f"{icon} <b>ATR Graph Release Gate Cutover</b>",
        "",
        "Component: <code>release_gate</code>",
        f"Status: <code>{status.upper()}</code>",
        "Evidence:",
        f"  • 7d critical drifts: <code>{summary.get('critical_drifts_7d', '?')}</code>",
        f"  • Decision match: <code>{summary.get('pct_decision_match', 0):.0f}%</code>",
        f"  • Missing replay cert edge (live): <code>{summary.get('missing_replay_cert_edge_live_critical', '?')}</code>",
    ]
    return "\n".join(lines)
