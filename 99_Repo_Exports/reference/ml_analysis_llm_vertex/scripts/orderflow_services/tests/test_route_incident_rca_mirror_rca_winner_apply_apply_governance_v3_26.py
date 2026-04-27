import pytest
import time
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_slo_analytics_v3_26 import calculate_analytics
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_auto_escalation_summarizer_v3_26 import calculate_severity

def test_calculate_analytics():
    decisions = [{"apply_id": "app-1", "decision": "APPLY_VERTEX"}]
    journal = [{"apply_id": "app-1"}]
    # keep rate
    verify = [
        {"apply_id": "app-0", "decision": "KEEP_APPLIED"},
        {"apply_id": "app-1", "decision": "ROLLBACK_PREVIOUS_POLICY", "ts_ms": "10000"}
    ]
    rb_journal = [
        {"apply_id": "app-1", "ts_ms": "12000"}
    ]
    
    ar, vkr, rb50, rb95 = calculate_analytics(decisions, journal, verify, rb_journal)
    assert ar == 1.0
    assert abs(vkr - 0.5) < 0.01
    assert rb50 == 2.0
    assert rb95 == 2.0

def test_calculate_severity_critical():
    slo = {"verify_keep_rate": "1.0", "rollback_mttr_p95_sec": "50.0"}
    retry = {"status": "exhausted"}
    sev, msg = calculate_severity(slo, retry)
    assert sev == "critical"

def test_calculate_severity_warning_mttr():
    slo = {"verify_keep_rate": "1.0", "rollback_mttr_p95_sec": "150.0"}
    retry = {"status": "ok"}
    sev, msg = calculate_severity(slo, retry)
    assert sev == "warning"
    
def test_calculate_severity_warning_vkr():
    slo = {"verify_keep_rate": "0.50", "rollback_mttr_p95_sec": "50.0"}
    retry = {"status": "ok"}
    sev, msg = calculate_severity(slo, retry)
    assert sev == "warning"

def test_calculate_severity_info():
    slo = {"verify_keep_rate": "0.95", "rollback_mttr_p95_sec": "50.0"}
    retry = {"status": "ok"}
    sev, msg = calculate_severity(slo, retry)
    assert sev == "info"
