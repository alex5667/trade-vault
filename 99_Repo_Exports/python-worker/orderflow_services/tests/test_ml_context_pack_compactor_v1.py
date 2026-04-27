from orderflow_services.ml_context_pack_compactor_v1 import compact_request


def test_compact_request_keeps_core_fields_only():
    req = {
        "request_id": "r1",
        "ts_ms": 123,
        "task_type": "root_cause_degradation",
        "payload": {
            "model_snapshot": {
                "model_id": "m1",
                "family": "edge_stack_v1",
                "kind": "edge_stack_v1",
                "hot_symbols_json": ["BTCUSDT"],
                "status": "warning",
                "reason_codes_json": ["HIGH_ERROR_RATE"],
                "latency_p95_max_ms": 4.2,
            },
            "training": {
                "run_id": "t1",
                "sample_n": 100,
                "pos_rate": 0.2,
                "metrics_json": {"brier": 0.12},
            },
        },
    }
    out = compact_request(req)
    assert out["scope"]["model_id"] == "m1"
    assert out["context"]["status"] == "warning"
    assert out["prompt_version"]
    assert out["policy_version"]
    assert out["compact_hash"]

