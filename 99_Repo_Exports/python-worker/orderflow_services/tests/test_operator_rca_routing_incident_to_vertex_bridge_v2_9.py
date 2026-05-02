from orderflow_services.operator_rca_routing_incident_to_vertex_bridge_v2_9 import (
    build_routing_incident_rca_pack,
    compact_timeline,
)


def test_compact_timeline_keeps_order_and_limits_size():
    timeline = [{"ts_ms": i, "event_type": "x"} for i in range(50)]
    compact = compact_timeline(timeline, limit=10)
    assert len(compact) == 10
    assert compact[0]["ts_ms"] == 0
    assert compact[-1]["ts_ms"] == 49


def test_build_routing_incident_rca_pack_extracts_core_fields():
    row = {
        "severity": "critical",
        "bundle_json": """,
        {
          "route_change_id": "rc-9",
          "bundle_hash": "abc123",
          "primary_reason_codes": ["ERROR_RATE_SPIKE", "ROLLBACK_FAILED"],
          "summary": {"apply_results_n": 1},
          "route_diff_json": {"model_name": {"before": "a", "after": "b"}},
          "timeline_json": [{"ts_ms": 100, "event_type": "APPLY"}, {"ts_ms": 200, "event_type": "VERIFY"}],
          "sections_json": {"apply_results": [{"x": 1}], "verify_results": [{"y": 2}]},
          "baseline_route_json": {"provider": "vertex", "model_name": "gemini-2.5-flash-lite"},
          "current_route_json": {"provider": "vertex", "model_name": "gemini-2.5-flash"}
        }
        """
    }
    pack = build_routing_incident_rca_pack(row)
    assert pack["route_change_id"] == "rc-9"
    assert pack["severity"] == "critical"
    assert "ERROR_RATE_SPIKE" in pack["primary_reason_codes"]
    assert pack["section_counts"]["apply_results"] == 1
    assert pack["baseline_route"]["model_name"] == "gemini-2.5-flash-lite"
    assert pack["current_route"]["model_name"] == "gemini-2.5-flash"
    assert pack["compact_hash"]
