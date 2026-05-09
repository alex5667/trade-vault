from core.seq_dq_v1 import surface_dq_indicators


class MockRuntime:
    pass

def test_surface_dq_indicators_v1():
    runtime = MockRuntime()
    indicators = {}

    # Initial state
    out = surface_dq_indicators(runtime, indicators)
    assert out["tick_gap_count"] == 0
    assert out["tick_dup_count"] == 0
    assert out["tick_reorder_count"] == 0
    assert out["tick_seq_last_reason"] == 0
    assert out["tick_missing_seq_ema"] == 0.0
    assert out["tick_gap_p50_ms"] == 0.0
    assert out["tick_gap_p95_ms"] == 0.0

    # Populated state
    runtime.tick_id_gap_count = 10
    runtime.tick_id_dup_count = 5
    runtime.tick_id_reorder_count = 1
    runtime.tick_id_last_reason = 3
    runtime.tick_missing_seq_ema = 0.5
    runtime.tick_gap_p50_ms = 120.0
    runtime.tick_gap_p95_ms = 450.0

    out = surface_dq_indicators(runtime, indicators)
    assert out["tick_gap_count"] == 10
    assert out["tick_dup_count"] == 5
    assert out["tick_reorder_count"] == 1
    assert out["tick_seq_last_reason"] == 3
    assert out["tick_missing_seq_ema"] == 0.5
    assert out["tick_gap_p50_ms"] == 120.0
    assert out["tick_gap_p95_ms"] == 450.0
