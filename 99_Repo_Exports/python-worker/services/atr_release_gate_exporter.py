import logging
from prometheus_client import Counter, Gauge, start_http_server

from services.analytics_db import get_conn

logger = logging.getLogger("atr_release_gate_exporter")

# ── Legacy metrics ──────────────────────────────────────────────────────────
ATR_RELEASE_SCORECARDS_TOTAL = Counter(
    "atr_release_scorecards_total",
    "Total number of scorecards generated",
    ["decision"]
)

ATR_RELEASE_READINESS_SCORE = Gauge(
    "atr_release_readiness_score",
    "Readiness score of the latest evaluated scorecard",
    ["change_id"]
)

ATR_RELEASE_BLOCKERS_TOTAL = Counter(
    "atr_release_blockers_total",
    "Total number of blockers triggered",
    ["reason_code"]
)

ATR_RELEASE_WARNINGS_TOTAL = Counter(
    "atr_release_warnings_total",
    "Total number of warnings triggered",
    ["reason_code"]
)

ATR_RELEASE_OVERRIDE_TOTAL = Counter(
    "atr_release_override_total",
    "Total number of release overrides"
)

ATR_RELEASE_DENIED_TOTAL = Counter(
    "atr_release_denied_total",
    "Total number of release denials",
    ["change_type", "risk_level"]
)

# ── Phase 8.2 — graph release gate metrics ──────────────────────────────────

ATR_RELEASE_GRAPH_EQUIVALENCE_TOTAL = Counter(
    "atr_release_graph_equivalence_total",
    "Total equivalence checks between legacy and graph release gate",
    ["status"]  # passed | failed
)

ATR_RELEASE_GRAPH_DRIFT_TOTAL = Counter(
    "atr_release_graph_drift_total",
    "Total release drifts detected by graph gate",
    ["drift_kind", "severity"]
)

ATR_RELEASE_GRAPH_DECISION_MISMATCH_TOTAL = Counter(
    "atr_release_graph_decision_mismatch_total",
    "Total release decision mismatches (critical: legacy != graph)"
)

ATR_RELEASE_GRAPH_MISSING_CERT_EDGE_TOTAL = Counter(
    "atr_release_graph_missing_cert_edge_total",
    "Missing replay/rollout cert edges detected by graph gate",
    ["target_stage", "cert_kind"]
)

ATR_RELEASE_GRAPH_MISSING_BLOCKER_TOTAL = Counter(
    "atr_release_graph_missing_blocker_total",
    "Missing freeze/override blockers detected by graph gate"
)

ATR_RELEASE_GRAPH_CUTOVER_READINESS = Gauge(
    "atr_release_graph_cutover_readiness_total",
    "Current cutover readiness status (1 = active for this status)",
    ["status"]
)


# ── Metric update helpers ───────────────────────────────────────────────────

def update_metrics_from_scorecard(scorecard: dict):
    """Update prometheus metrics given a new scorecard.
    Called every time a scorecard is generated or during periodic poll.
    """
    try:
        decision = scorecard.get("decision", "unknown")
        ATR_RELEASE_SCORECARDS_TOTAL.labels(decision=decision).inc()

        change_id = scorecard.get("change_id", "unknown")
        score = scorecard.get("readiness_score", 0.0)
        ATR_RELEASE_READINESS_SCORE.labels(change_id=change_id).set(score)

        blockers = scorecard.get("blockers", [])
        for b in blockers:
            ATR_RELEASE_BLOCKERS_TOTAL.labels(reason_code=b).inc()

        warnings = scorecard.get("warnings", [])
        for w in warnings:
            ATR_RELEASE_WARNINGS_TOTAL.labels(reason_code=w).inc()

    except Exception as e:
        logger.error(f"Failed to update metrics from scorecard: {e}")


def update_metrics_from_decision(decision_record: dict, change_type: str, risk_level: str):
    try:
        action = decision_record.get("action", "")
        if action == "override_release":
            ATR_RELEASE_OVERRIDE_TOTAL.inc()
        elif action == "deny_release":
            ATR_RELEASE_DENIED_TOTAL.labels(change_type=change_type, risk_level=risk_level).inc()
    except Exception as e:
        logger.error(f"Failed to update metrics from decision: {e}")


def update_metrics_from_graph_check(scorecard: dict) -> None:
    """
    Phase 8.2: Update graph-specific metrics from a decide_release() result
    that has been enriched with _graph_source, _compare, _critical_drifts.
    """
    try:
        compare         = scorecard.get("_compare")
        critical_drifts = scorecard.get("_critical_drifts", [])
        graph_source    = scorecard.get("_graph_source", "legacy")

        if compare is None:
            return  # graph gate disabled or out-of-pilot

        status = "passed" if compare.get("matching") else "failed"
        ATR_RELEASE_GRAPH_EQUIVALENCE_TOTAL.labels(status=status).inc()

        if not compare.get("matching"):
            ATR_RELEASE_GRAPH_DECISION_MISMATCH_TOTAL.inc()

        for drift in compare.get("drifts", []):
            kind     = drift.get("drift_kind", "unknown")
            severity = drift.get("severity", "warn")
            ATR_RELEASE_GRAPH_DRIFT_TOTAL.labels(drift_kind=kind, severity=severity).inc()

            if kind in ("missing_replay_cert_edge", "missing_rollout_cert_edge"):
                graph_state = scorecard.get("_graph_state") or {}
                rel_state   = graph_state.get("release_state", {})
                stage       = rel_state.get("target_stage", "unknown")
                ATR_RELEASE_GRAPH_MISSING_CERT_EDGE_TOTAL.labels(
                    target_stage=stage,
                    cert_kind=kind.replace("missing_", "").replace("_edge", ""),
                ).inc()

            elif kind in ("missing_freeze_blocker", "missing_override_constraint"):
                ATR_RELEASE_GRAPH_MISSING_BLOCKER_TOTAL.inc()

    except Exception as exc:
        logger.error("update_metrics_from_graph_check failed: %s", exc)


def update_cutover_readiness_metric(status: str) -> None:
    """Set cutover readiness gauge (only the active status = 1)."""
    all_statuses = ["not_ready", "shadow_healthy", "ready_for_read", "ready_for_enforce"]
    for s in all_statuses:
        ATR_RELEASE_GRAPH_CUTOVER_READINESS.labels(status=s).set(1 if s == status else 0)


def start_exporter(port: int = 9835):
    """Start the prometheus exporter server asynchronously."""
    try:
        start_http_server(port)
        logger.info(f"ATR Release Gate Exporter started on port {port}")
    except Exception as e:
        logger.error(f"Failed to start prometheus exporter: {e}")

