from orderflow_services.operator_rca_results_persister_v2_1 import RCAResult, _json_hash, build_quality_event


def test_json_hash_stable():
    payload = {"b": 2, "a": 1}
    assert _json_hash(payload) == _json_hash({"a": 1, "b": 2})

def test_build_quality_event_fields():
    result = RCAResult(
        recommendation_id="rec-1",
        ts_ms=1,
        provider="vertex",
        model_name="gemini",
        status="ok",
        latency_ms=123,
        estimated_cost_usd=0.01,
        output_json={"findings": [{"kind": "drift"}], "recommendations": [{"action": "open_incident"}]},
        prompt_version="p1",
        policy_version="pol1",
    )
    evt = build_quality_event(result)
    assert evt["recommendation_id"] == "rec-1"
    assert evt["findings_n"] == "1"
    assert evt["recommendations_n"] == "1"
