from orderflow_services.route_incident_rca_mirror_auto_escalation_summarizer_v3_10 import decide_severity
from orderflow_services.route_incident_rca_mirror_slo_analytics_v3_10 import compute_rollup


def test_compute_rollup_detects_low_apply_rate():
    decisions = [
        {"ts_ms": "9999999999999", "controller_decision": "PROMOTE"},
        {"ts_ms": "9999999999999", "controller_decision": "ROLLBACK"},
    ]
    journal = [
        {"ts_ms": "9999999999999", "transition_type": "AUDIT_TO_MIRROR"},
    ]
    verification = []
    rollup = compute_rollup(decisions, journal, verification)
    assert rollup["promotion_apply_rate"] == 1.0
    assert rollup["rollback_apply_rate"] == 0.0
    assert "ROLLBACK_APPLY_RATE_LOW" in rollup["reason_codes"]


def test_decide_severity_critical_on_retry_exhausted():
    slo = {"promotion_apply_rate": "1.0", "rollback_apply_rate": "1.0", "rollback_mttr_p95_sec": "10"}
    retry = {"decision": "EXHAUSTED", "reason_code": "MAX_ATTEMPTS_REACHED"}
    summary = decide_severity(slo, retry)
    assert summary["severity"] == "critical"
    assert "RETRY_EXHAUSTED" in summary["reason_codes"]


def test_decide_severity_warning_on_mttr_high():
    slo = {"promotion_apply_rate": "1.0", "rollback_apply_rate": "1.0", "rollback_mttr_p95_sec": "130"}
    retry = {"decision": "NOOP", "reason_code": "NO_ACTION"}
    summary = decide_severity(slo, retry)
    assert summary["severity"] == "warning"
