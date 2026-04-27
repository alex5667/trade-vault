from __future__ import annotations

from orderflow_services.ml_health_rollup_worker_v1 import (
    _hist_quantile,
    _latency_bucket_index,
    _model_id,
    _status,
)


def test_model_id_uses_kind_and_run_id():
    fields = {"kind": "edge_stack_v1", "model_run_id": "run42"}
    assert _model_id(fields) == "edge_stack_v1:run42"


def test_status_prefers_status_field():
    assert _status({"status": "SHADOW", "allow": "1"}) == "SHADOW"
    assert _status({"allow": "1"}) == "ALLOW"
    assert _status({"allow": "0"}) == "DENY"


def test_latency_bucket_index_monotonic():
    assert _latency_bucket_index(0.1) <= _latency_bucket_index(1.0)
    assert _latency_bucket_index(1.0) <= _latency_bucket_index(100.0)


def test_hist_quantile_approximation():
    counts = [0] * 13
    counts[2] = 10  # <=1ms
    counts[4] = 10  # <=5ms
    assert _hist_quantile(counts, 0.50) in (1.0, 2.0)
    assert _hist_quantile(counts, 0.95) >= 5.0
