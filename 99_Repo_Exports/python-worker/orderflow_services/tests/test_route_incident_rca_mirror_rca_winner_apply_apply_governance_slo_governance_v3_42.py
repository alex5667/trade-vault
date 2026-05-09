from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_auto_escalation_summarizer_v3_42 import (
    decide_severity,
)
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_slo_analytics_v3_42 import (
    compute_rollup,
)


def test_compute_rollup_detects_low_verify_keep_rate():
    decisions = [
        {"ts_ms": "9999999999999", "decision": "APPLY_PRIMARY_ARM_SHADOW"},
        {"ts_ms": "9999999999999", "decision": "APPLY_SINGLE_ARM"},
    ]
    journal = [
        {"ts_ms": "9999999999999", "decision": "APPLY_PRIMARY_ARM_SHADOW"},
        {"ts_ms": "9999999999999", "decision": "APPLY_SINGLE_ARM"},
    ]
    verification = [
        {"ts_ms": "9999999999999", "decision": "KEEP_APPLIED"},
    ]
    rollback = []
    rollup = compute_rollup(decisions, journal, verification, rollback)
    assert rollup["apply_rate"] == 1.0
    assert rollup["verify_keep_rate"] == 0.5
    assert "VERIFY_KEEP_RATE_LOW" in rollup["reason_codes"]


def test_decide_severity_critical_on_retry_exhausted():
    slo = {"apply_rate": "1.0", "verify_keep_rate": "1.0", "rollback_mttr_p95_sec": "10"}
    retry = {"decision": "EXHAUSTED", "reason_code": "MAX_ATTEMPTS_REACHED"}
    summary = decide_severity(slo, retry)
    assert summary["severity"] == "critical"
    assert "RETRY_EXHAUSTED" in summary["reason_codes"]


def test_decide_severity_warning_on_mttr_high():
    slo = {"apply_rate": "1.0", "verify_keep_rate": "1.0", "rollback_mttr_p95_sec": "130"}
    retry = {"decision": "NOOP", "reason_code": "NO_ACTION"}
    summary = decide_severity(slo, retry)
    assert summary["severity"] == "warning"
