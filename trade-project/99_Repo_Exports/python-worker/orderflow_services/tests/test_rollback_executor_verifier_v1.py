from orderflow_services.rollback_executor_verifier_v1 import compute_rollback_verification


def test_verification_pass_when_metrics_recovered():
    baseline = {
        "error_rate_max": 0.01,
        "latency_p95_max_ms": 3.0,
        "missing_critical_rate_max": 0.01,
        "allow_rate_avg": 0.25,
    }
    after = {
        "error_rate_max": 0.011,
        "latency_p95_max_ms": 3.5,
        "missing_critical_rate_max": 0.01,
        "allow_rate_avg": 0.22,
    }
    d = compute_rollback_verification(baseline, after, {})
    assert d.verification_status == "PASS"
    assert d.reason_codes == []


def test_verification_fail_on_error_rate_or_latency_spike():
    baseline = {
        "error_rate_max": 0.01,
        "latency_p95_max_ms": 3.0,
        "missing_critical_rate_max": 0.01,
        "allow_rate_avg": 0.25,
    }
    after = {
        "error_rate_max": 0.05,
        "latency_p95_max_ms": 6.0,
        "missing_critical_rate_max": 0.01,
        "allow_rate_avg": 0.25,
    }
    d = compute_rollback_verification(baseline, after, {})
    assert d.verification_status == "FAIL"
    assert "ERROR_RATE_SPIKE" in d.reason_codes
    assert "LATENCY_P95_REGRESSION" in d.reason_codes


def test_verification_inconclusive_on_missing_baseline():
    d = compute_rollback_verification({}, {"error_rate_max": 0.01}, {})
    assert d.verification_status == "INCONCLUSIVE"
    assert "MISSING_BASELINE_SNAPSHOT" in d.reason_codes
