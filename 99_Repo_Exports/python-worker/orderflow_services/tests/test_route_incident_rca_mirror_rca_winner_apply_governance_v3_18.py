import json
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_auto_escalation_summarizer_v3_18 import calculate_severity
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_retry_controller_v3_18 import is_cfg_match

def test_calculate_severity_critical_low_keep():
    # Keep rate 10%, apply rate > 0
    sev = calculate_severity(apply_rate=1.0, verify_keep_rate=0.1, rollback_mttr_p95=10.0, recent_retries=0)
    assert sev == "critical"

def test_calculate_severity_critical_high_retries():
    # 6 retries in last hour
    sev = calculate_severity(apply_rate=0.0, verify_keep_rate=1.0, rollback_mttr_p95=10.0, recent_retries=6)
    assert sev == "critical"

def test_calculate_severity_warning_mttr():
    # MTTR > 120 but < 360
    sev = calculate_severity(apply_rate=0.0, verify_keep_rate=1.0, rollback_mttr_p95=121.0, recent_retries=0)
    assert sev == "warning"

def test_calculate_severity_info():
    # Good numbers
    sev = calculate_severity(apply_rate=1.0, verify_keep_rate=1.0, rollback_mttr_p95=10.0, recent_retries=1)
    assert sev == "info"

def test_is_cfg_match_true():
    live = {"mode": "SHADOW", "primary_arm": "deterministic", "shadow_arms": '["vertex_candidate"]'}
    target = {"mode": "SHADOW", "primary_arm": "deterministic", "shadow_arms": '["vertex_candidate"]'}
    assert is_cfg_match(live, target) is True
    
def test_is_cfg_match_false_shadows():
    live = {"mode": "SHADOW", "primary_arm": "deterministic", "shadow_arms": '["vertex_candidate"]'}
    target = {"mode": "SHADOW", "primary_arm": "deterministic", "shadow_arms": '["local_fallback_candidate"]'}
    assert is_cfg_match(live, target) is False
