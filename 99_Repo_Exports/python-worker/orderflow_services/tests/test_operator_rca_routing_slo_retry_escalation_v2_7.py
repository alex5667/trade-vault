from orderflow_services.operator_rca_routing_slo_analytics_v2_7 import _reason_codes
from orderflow_services.rollback_retry_controller_v2_7 import _backoff_sec, RETRYABLE, NON_RETRYABLE
from orderflow_services.rollback_auto_escalation_summarizer_v2_7 import _severity


def test_reason_codes():
    cfg = {"success_rate_min": 0.8, "mttr_p95_max_sec": 900}
    codes = _reason_codes(0.5, 1200, 2, cfg)
    assert "ROUTE_VERIFY_SUCCESS_RATE_LOW" in codes
    assert "ROUTE_VERIFY_MTTR_P95_HIGH" in codes
    assert "ROUTE_VERIFY_SLO_BREACH" in codes


def test_backoff_bounded():
    assert _backoff_sec(1, 60, 900) == 60
    assert _backoff_sec(2, 60, 900) == 120
    assert _backoff_sec(10, 60, 900) == 900


def test_retry_reason_sets():
    assert "ROUTE_PROVIDER_TIMEOUT" in RETRYABLE
    assert "ROUTE_POLICY_DENIED" in NON_RETRYABLE


def test_escalation_severity():
    cfg = {"warning_open_threshold": 3, "critical_open_threshold": 5}
    assert _severity(1, 0, cfg) == "info"
    assert _severity(3, 0, cfg) == "warning"
    assert _severity(6, 5, cfg) == "critical"
