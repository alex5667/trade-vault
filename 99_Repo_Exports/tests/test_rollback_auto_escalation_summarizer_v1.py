from orderflow_services.rollback_auto_escalation_summarizer_v1 import build_auto_escalation_summary


def test_escalation_warning_for_slo_breach():
    rows = [
        {"recommendation_id": "r1", "final_state": "ROLLBACK_FAILED", "requested_ts_ms": 0, "terminal_ts_ms": 1000000},
        {"recommendation_id": "r2", "final_state": "ROLLBACK_SUCCESS", "requested_ts_ms": 0, "terminal_ts_ms": 1000000},
    ]
    out = build_auto_escalation_summary(rows, mttr_slo_sec=900, success_rate_floor=0.95)
    assert out.severity in {"warning", "critical"}
    assert "ROLLBACK_SUCCESS_RATE_LOW" in out.reason_codes


def test_escalation_critical_on_many_failed_ids():
    rows = [
        {"recommendation_id": "r1", "final_state": "ROLLBACK_FAILED", "requested_ts_ms": 0, "terminal_ts_ms": 10},
        {"recommendation_id": "r2", "final_state": "ROLLBACK_FAILED", "requested_ts_ms": 0, "terminal_ts_ms": 10},
        {"recommendation_id": "r3", "final_state": "MANUAL_REVIEW", "requested_ts_ms": 0, "terminal_ts_ms": 10},
    ]
    out = build_auto_escalation_summary(rows)
    assert out.severity == "critical"
