from orderflow_services.llm_recommendation_guard_v1 import guard_recommendations


def test_guard_allows_only_whitelist():
    payload = {
        "schema_version": 1,
        "analysis_run_id": "r1",
        "status": "ok",
        "summary": "s",
        "findings": [],
        "recommendations": [
            {"action": "require_shadow_retrain", "target": "edge_stack_v1", "risk": "low", "reason_code": "DRIFT"},
            {"action": "change_execution_caps", "target": "cfg", "risk": "high", "reason_code": "NOPE"},
        ],
    }
    out = guard_recommendations(payload)
    assert out["valid"] is True
    assert len(out["guarded_recommendations"]) == 1
    assert out["guarded_recommendations"][0]["action"] == "require_shadow_retrain"
    assert len(out["blocked_recommendations"]) == 1


def test_guard_rejects_invalid_shape():
    out = guard_recommendations({"status": "ok"})
    assert out["valid"] is False
    assert "missing_schema_version" in out["errors"]
