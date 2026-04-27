import pytest
from services.orderflow.decision_record_v1 import extract_fields_best_effort

def test_extract_ml_fields():
    stub = {
        "evidence": {
            "ml": {
                "mode": "live",
                "p_edge_cal": 0.62,
                "latency_us": 12000,
                "allow": 1
            }
        }
    }
    res = extract_fields_best_effort(stub)
    assert res["ml_enabled"] is True
    assert res["ml_p_cal"] == 0.62
    assert res["ml_latency_ms"] == 12.0
    assert res["ml_state"] == "allow"
