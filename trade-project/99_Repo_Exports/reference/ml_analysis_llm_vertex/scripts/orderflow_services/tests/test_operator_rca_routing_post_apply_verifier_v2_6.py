from orderflow_services.operator_rca_routing_post_apply_verifier_v2_6 import evaluate_verification


def test_evaluate_verification_pass():
    status, reasons, rollback_required = evaluate_verification(
        {
            "exposures_n": 10,
            "usefulness_avg": 0.8,
            "error_rate": 0.05,
            "parse_fail_rate": 0.01,
        }
    )
    assert status == "PASS"
    assert rollback_required is False
    assert reasons == ["VERIFY_PASS"]


def test_evaluate_verification_rollback_required():
    status, reasons, rollback_required = evaluate_verification(
        {
            "exposures_n": 10,
            "usefulness_avg": 0.1,
            "error_rate": 0.30,
            "parse_fail_rate": 0.20,
        }
    )
    assert status == "ROLLBACK_REQUIRED"
    assert rollback_required is True
    assert "ERROR_RATE_SPIKE" in reasons
    assert "USEFULNESS_DROP" in reasons


def test_evaluate_verification_inconclusive_low_exposure():
    status, reasons, rollback_required = evaluate_verification(
        {
            "exposures_n": 1,
            "usefulness_avg": 0.9,
            "error_rate": 0.0,
            "parse_fail_rate": 0.0,
        }
    )
    assert status == "INCONCLUSIVE"
    assert rollback_required is False
    assert reasons == ["LOW_EXPOSURE"]
