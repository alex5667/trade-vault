import json

from orderflow_services.route_incident_rca_mirror_vertex_feedback_governor_v3_13 import compute_decision
from orderflow_services.route_incident_rca_mirror_vertex_rca_consumer_v3_13 import build_deterministic_result


def test_build_deterministic_result():
    bundle_json = json.dumps({"bundle_id": "test-123", "severity": "warning"})
    result = build_deterministic_result(bundle_json)

    assert result["bundle_id"] == "test-123"
    assert result["severity"] == "warning"
    assert "request_id" in result
    assert result["confidence"] == 0.85
    assert "Latency spike observed" in result["dominant_findings"]

def test_compute_decision_less_than_min_samples():
    samples = [{"quality_score": 1.0}] * 5
    decision, rollups = compute_decision(samples)
    assert decision == "HOLD"
    assert rollups == {}

def test_compute_decision_keep_auto():
    samples = [{"quality_score": 0.8, "usefulness_score": 0.8, "accepted": 1}] * 15
    decision, rollups = compute_decision(samples)
    assert decision == "KEEP_AUTO"
    assert rollups["avg_quality"] == 0.8
    assert rollups["avg_usefulness"] == 0.8
    assert rollups["accepted_rate"] == 1.0
    assert rollups["low_quality_rate"] == 0.0

def test_compute_decision_prefer_local_only_due_to_quality():
    samples = [{"quality_score": 0.4, "usefulness_score": 0.8, "accepted": 1}] * 15
    decision, rollups = compute_decision(samples)
    assert decision == "PREFER_LOCAL_ONLY"

def test_compute_decision_prefer_local_only_due_to_usefulness():
    samples = [{"quality_score": 0.8, "usefulness_score": 0.4, "accepted": 1}] * 15
    decision, rollups = compute_decision(samples)
    assert decision == "PREFER_LOCAL_ONLY"

def test_compute_decision_prefer_local_only_due_to_accepted_rate():
    samples = [{"quality_score": 0.8, "usefulness_score": 0.8, "accepted": 0}] * 15
    decision, rollups = compute_decision(samples)
    assert decision == "PREFER_LOCAL_ONLY"
