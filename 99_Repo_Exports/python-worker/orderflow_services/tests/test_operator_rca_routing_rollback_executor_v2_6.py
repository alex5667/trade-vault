from orderflow_services.operator_rca_routing_rollback_executor_v2_6 import _parse


def test_parse_rollback_request():
    req = _parse(
        {
            b"recommendation_id": b"rec-1",
            b"ts_ms": b"1000",
            b"rollback_type": b"ROUTE_DEFAULT_ROLLBACK",
            b"baseline_route_json": b'{"provider": "vertex"}',
            b"applied_route_json": b'{"provider": "vertex", "model_name": "gemini-2.5-flash"}',
            b"reason_codes_json": b'["USEFULNESS_DROP"]',
        }
    )
    assert req.recommendation_id == "rec-1"
    assert req.rollback_type == "ROUTE_DEFAULT_ROLLBACK"
    assert 'vertex' in req.baseline_route_json
