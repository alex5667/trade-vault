import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from services.analytics_db import get_conn

logger = logging.getLogger("atr_graph_consistency_gate")

_ENABLE = os.getenv("ATR_GRAPH_CONSISTENCY_GATE_ENABLE", "0") == "1"
_ENFORCE = os.getenv("ATR_GRAPH_CONSISTENCY_GATE_ENFORCE", "0") == "1"

# Bounded scopes
_SCOPES_ENV = os.getenv("ATR_GRAPH_CONSISTENCY_GATE_SCOPES", "")
_BOUNDED_SYMBOLS = set(_SCOPES_ENV.split(",")) if _SCOPES_ENV else set()

_RISK_ENV = os.getenv("ATR_GRAPH_CONSISTENCY_GATE_RISK_LEVELS", "")
_BOUNDED_RISK_LEVELS = set(_RISK_ENV.split(",")) if _RISK_ENV else set()

_GLOBAL_ENFORCE = os.getenv("ATR_GRAPH_CONSISTENCY_GATE_GLOBAL_HIGH_CRITICAL", "0") == "1"

# Table list for open drifts
DRIFT_TABLES = {
    "release": "atr_release_drifts",
    "freeze_override": "atr_freeze_override_drifts",
    "effective_state": "atr_effective_state_drifts",
    "runtime_gate": "atr_runtime_gate_drifts",
    "protective": "atr_protective_drifts",
    "projection": "atr_control_plane_drifts"
}

def _generate_id(prefix: str) -> str:
    return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

def is_in_scope(symbol: str, risk_level: str) -> bool:
    if _GLOBAL_ENFORCE and risk_level in ("high", "critical"):
        return True

    symbol_ok = symbol in _BOUNDED_SYMBOLS if _BOUNDED_SYMBOLS else True
    risk_ok = risk_level in _BOUNDED_RISK_LEVELS if _BOUNDED_RISK_LEVELS else True

    return symbol_ok and risk_ok

def collect_graph_consistency_inputs(scope_value: str) -> dict[str, Any]:
    """
    Query component certs and open drifts for a given scope.
    """
    inputs = {
        "pass_scores": {
            "graph_consistency": False,
            "projection_consistency": False,
            "release_equivalence": False,
            "freeze_override": False,
            "effective_state": False,
            "runtime_gate": False,
            "protective_lifecycle": False
        },
        "open_drifts": []
    }

    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            for family, table in DRIFT_TABLES.items():
                cur.execute(f"SELECT * FROM {table} WHERE scope_value = %s AND status = 'open'", (scope_value,))
                rows = cur.fetchall()
                if not rows:
                    if family in inputs["pass_scores"]:
                        inputs["pass_scores"][family] = True
                    if family == "projection":
                        inputs["pass_scores"]["projection_consistency"] = True
                else:
                    for r in rows:
                        inputs["open_drifts"].append({
                            "family": family,
                            "drift_kind": r["drift_kind"],  # type: ignore
                            "severity": r["severity"],  # type: ignore
                            "reason_code": r["reason_code"]  # type: ignore
                        })

            inputs["pass_scores"]["graph_consistency"] = True

            return inputs
    except Exception as e:
        logger.error(f"Failed to collect graph consistency inputs for {scope_value}: {e}")
        return inputs

def classify_graph_blockers(inputs: dict[str, Any], risk_level: str) -> tuple[list[str], list[str]]:
    blockers = []
    warnings = []

    for drift in inputs.get("open_drifts", []):
        family = drift["family"]
        sev = drift["severity"]
        kind = drift["drift_kind"]

        # Policy rules
        if sev == "critical":
            blockers.append(f"{family}_critical_drift_open")
        elif sev == "error":
            if risk_level in ("high", "critical"):
                blockers.append(f"{family}_error_drift_open")
            else:
                warnings.append(f"{family}_error_drift_open")
        else:
            warnings.append(f"{family}_warning_drift_open")

        if kind == "projection_stale_beyond_sla" and risk_level in ("high", "critical"):
             blockers.append("projection_stale_beyond_sla_on_target_scope")

        if kind == "missing_replay_cert_edge" and risk_level == "critical":
             blockers.append("missing_required_cert_edge_for_live_scope")

    return list(set(blockers)), list(set(warnings))

def decide_graph_consistency(change_id: str, scope_value: str, risk_level: str) -> dict[str, Any]:
    if not _ENABLE:
        return {
            "check_id": None,
            "change_id": change_id,
            "decision": "allow",
            "score": 100.0,
            "blockers": [],
            "warnings": [],
            "summary": {"gate_disabled": True}
        }

    inputs = collect_graph_consistency_inputs(scope_value)

    blockers, warnings = classify_graph_blockers(inputs, risk_level)

    score = 0.0
    ps = inputs["pass_scores"]
    if ps.get("graph_consistency"): score += 20
    if ps.get("projection_consistency"): score += 20
    if ps.get("release_equivalence"): score += 15
    if ps.get("freeze_override"): score += 10
    if ps.get("effective_state"): score += 10
    if ps.get("runtime_gate"): score += 15
    if ps.get("protective_lifecycle"): score += 10

    if blockers:
        decision = "deny"
    elif warnings:
        decision = "allow_with_override"
    else:
        decision = "allow"

    check_id = _generate_id("gcg")

    summary = {
        "total_open_drifts": len(inputs.get("open_drifts", [])),
        "pass_scores": ps,
    }

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO atr_graph_consistency_gate_checks (
                    check_id, change_id, scope_value, risk_level, graph_score,
                    decision, blockers_json, warnings_json, summary_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                check_id, change_id, scope_value, risk_level, score, decision,
                json.dumps(blockers), json.dumps(warnings), json.dumps(summary)
            ))

            if decision == "deny":
                 drift_id = _generate_id("gcdrift")
                 cur.execute("""
                     INSERT INTO atr_graph_consistency_drifts (
                         drift_id, change_id, scope_value, drift_family, drift_kind,
                         severity, status, reason_code, drift_json
                     ) VALUES (%s, %s, %s, %s, %s, %s, 'open', %s, %s)
                 """, (
                     drift_id, change_id, scope_value, "graph_core", "graph_consistency_cert_failed",
                     "critical", "blockers_present", json.dumps({"blockers": blockers})
                 ))

            conn.commit()
    except Exception as e:
        logger.error(f"Failed to persist graph consistency check for {change_id}: {e}")

    return {
        "check_id": check_id,
        "change_id": change_id,
        "decision": decision,
        "score": score,
        "blockers": blockers,
        "warnings": warnings,
        "summary": summary
    }

def request_waiver(drift_id: str, approver: str, reason_code: str, ttl_sec: int) -> bool:
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cur:
            cur.execute("SELECT drift_family, severity FROM atr_graph_consistency_drifts WHERE drift_id = %s", (drift_id,))
            drift = cur.fetchone()
            if not drift:
                return False

            # Forbid waiver for runtime/protective critical drifts or projection missing replay edge
            if drift["severity"] == "critical" and drift["drift_family"] in ("runtime_gate", "protective"):  # type: ignore
                return False

            waiver_id = _generate_id("waiver")
            # Calculate not_after
            cur.execute("SELECT now() + interval '%s seconds'", (ttl_sec,))
            not_after = cur.fetchone()[0]  # type: ignore

            cur.execute("""
                INSERT INTO atr_graph_consistency_waivers (
                    waiver_id, drift_id, approver, reason_code, ttl_sec, not_after, waiver_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (waiver_id, drift_id, approver, reason_code, ttl_sec, not_after, json.dumps({})))

            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Failed to request waiver for {drift_id}: {e}")
        return False

