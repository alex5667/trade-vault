from orderflow_services.operator_rca_feedback_loop_v2_1 import usefulness_from_feedback

def test_usefulness_mapping():
    assert usefulness_from_feedback("VERY_USEFUL") == 1.0
    assert usefulness_from_feedback("USEFUL") == 0.75
    assert usefulness_from_feedback("MIXED") == 0.50
    assert usefulness_from_feedback("NOT_USEFUL") == 0.0
