from orderflow_services.rollback_slo_analytics_v1 import summarize_rollback_slo, build_slo_reason_codes


def test_summarize_rollback_slo_basic():
    rows = [
        {"final_state": "ROLLBACK_SUCCESS", "requested_ts_ms": 0, "terminal_ts_ms": 0},
        {"final_state": "ROLLBACK_SUCCESS", "requested_ts_ms": 1000, "terminal_ts_ms": 101000},
        {"final_state": "ROLLBACK_FAILED", "requested_ts_ms": 2000, "terminal_ts_ms": 1402000},
    ]
    out = summarize_rollback_slo(rows, mttr_slo_sec=900)
    assert out.total == 3
    assert out.success == 2
    assert out.failed == 1
    assert out.breach_n == 1


def test_build_reason_codes():
    rows = [{"final_state": "ROLLBACK_FAILED", "requested_ts_ms": 0, "terminal_ts_ms": 1000000}]
    out = summarize_rollback_slo(rows, mttr_slo_sec=900)
    codes = build_slo_reason_codes(out, success_rate_floor=0.95, mttr_p95_ceiling_sec=900)
    assert "ROLLBACK_SUCCESS_RATE_LOW" in codes
