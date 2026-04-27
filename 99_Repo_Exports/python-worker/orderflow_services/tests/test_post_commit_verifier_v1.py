from orderflow_services.post_commit_verifier_v1 import evaluate_post_commit, build_verification_policy


def test_evaluate_post_commit_pass():
    policy = build_verification_policy("propose_threshold_canary")
    before = {"allow_rate_avg": 0.31, "error_rate_max": 0.01, "latency_p95_max_ms": 3.0}
    after = {"allow_rate_avg": 0.29, "error_rate_max": 0.02, "latency_p95_max_ms": 4.0, "signals_n": 100}
    status, reasons = evaluate_post_commit(
        action_type="propose_threshold_canary",
        before_snapshot=before,
        after_snapshot=after,
        policy=policy,
    )
    assert status == "PASS"
    assert reasons == []


def test_evaluate_post_commit_rollback_required():
    policy = build_verification_policy("propose_threshold_canary")
    before = {"allow_rate_avg": 0.31, "error_rate_max": 0.01, "latency_p95_max_ms": 3.0}
    after = {"allow_rate_avg": 0.18, "error_rate_max": 0.12, "latency_p95_max_ms": 10.0, "signals_n": 100}
    status, reasons = evaluate_post_commit(
        action_type="propose_threshold_canary",
        before_snapshot=before,
        after_snapshot=after,
        policy=policy,
    )
    assert status == "ROLLBACK_REQUIRED"
    assert "ERROR_RATE_SPIKE" in reasons
