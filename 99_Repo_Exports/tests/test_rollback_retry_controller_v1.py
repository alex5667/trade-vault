from orderflow_services.rollback_retry_controller_v1 import compute_retry_decision


def test_retry_eligible_timeout():
    out = compute_retry_decision({"attempt": 0, "failure_reason": "ROLLBACK_EXECUTOR_TIMEOUT"}, max_attempts=2, base_backoff_sec=300)
    assert out.should_retry is True
    assert out.next_attempt == 1
    assert out.backoff_sec == 300


def test_retry_hard_stop():
    out = compute_retry_decision({"attempt": 0, "failure_reason": "ROLLBACK_TARGET_MISSING"})
    assert out.should_retry is False
    assert out.reason_code == "ROLLBACK_RETRY_HARD_STOP"
