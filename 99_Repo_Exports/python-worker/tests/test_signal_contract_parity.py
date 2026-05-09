from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.async_signal_publisher import AsyncSignalPublisher
from services.orderflow.signal_pipeline import SignalPipeline


@pytest.mark.asyncio
@patch("services.orderflow.signal_pipeline.atomic_xadd_async", new_callable=AsyncMock)
@patch("services.orderflow.signal_pipeline.aread_derivatives_context", new_callable=AsyncMock)
@patch("services.orderflow.signal_pipeline.aread_exec_health_auto_freeze", new_callable=AsyncMock)
@patch("services.orderflow.signal_pipeline.aread_exec_health_rollups", new_callable=AsyncMock)
async def test_signal_payload_quantity_parity(mock_exec, mock_freeze, mock_deriv, mock_xadd):
    # Mock dependencies
    publisher = MagicMock(spec=AsyncSignalPublisher)
    publisher.r = MagicMock()

    atr_cache = MagicMock()
    atr_cache.get_with_meta.return_value = (10.0, {})

    pipeline = SignalPipeline(publisher, atr_cache)
    pipeline._hard_dq_gate = None
    pipeline._rs_gate = None
    pipeline._atr_floor_gate = None

    # Mock runtime
    runtime = MagicMock()
    runtime.symbol = "BTCUSDT"
    runtime.config = {
        "stop_mode": "ATR",
        "stop_atr_mult": 1.0,
        "tp_rr": "1.3,2.0,2.7",
        "trail_after_tp1": False,
        "min_conf": 70,
        "min_lot": 0.01,
        "liq_gate_enabled": 0,
        "ev_gate_enabled": 0,
        "edge_cost_gate_enabled": 0,
    }
    runtime.get_atr_tf_selected.return_value = "1m"
    runtime.calibrated_specs = {}

    # Mock indicators and signal
    import time
    now_ms = int(time.time() * 1000)
    indicators = {"atr": 10.0, "lot": 0.5, "ts_ms": now_ms}
    signal = {
        "signal_id": "test_id",
        "direction": "LONG",
        "confidence": 0.8,  # 80%, should pass 70% threshold
        "entry": 50000.0,
        "sl": 49000.0,
        "ts_ms": now_ms,
        "tick_ts": now_ms,
    }

    pipeline._normalize_trailing_flag = MagicMock(return_value=False)

    mock_freeze.return_value = MagicMock(active=False)
    mock_exec.return_value = {}
    mock_deriv.return_value = {"basis_bps": 5.0, "funding_rate_z": 0.5, "oi_notional_usd": 1000000}

    # Also patch evaluate_derivatives_context to not veto
    with patch("services.orderflow.signal_pipeline.evaluate_derivatives_context") as mock_eval:
        mock_eval.return_value = MagicMock(is_vetoed=False, is_shadow=False, reason="OK", detail="mock")
        runtime.indicators = indicators
        await pipeline.publish_signal(runtime, signal)

    # Verify atomic_xadd_async was called with a payload containing qty and quantity
    assert mock_xadd.called
    # In atomic_xadd_async, signature is typically (redis, stream, fields, maxlen)
    args, kwargs = mock_xadd.call_args
    # Assuming fields is the 3rd arg or a kwarg 'fields'
    if 'fields' in kwargs:
        fields = kwargs['fields']
    elif len(args) > 2:
        fields = args[2]
    else:
        fields = args[1] # Depending on if 'redis' is passed.

    assert 'lot' in fields
    assert 'qty' in fields
    assert 'quantity' in fields

    # For BTCUSDT contract_size is 1.0
    assert float(fields['lot']) == 0.5
    assert float(fields['qty']) == 0.5
    assert float(fields['quantity']) == 0.5

    # ---------------------------------------------------------
    # Golden Fixture Assertions for P4 Latency Contract
    # ---------------------------------------------------------
    assert 'ts_event_ms' in fields, "Latency contract violation: ts_event_ms is missing"
    assert 'ts_emit_ms' in fields, "Latency contract violation: ts_emit_ms is missing"

    ts_event = int(fields['ts_event_ms'])
    ts_emit = int(fields['ts_emit_ms'])

    # SLO verification: emit delay must be < 10ms
    slo_delta = ts_emit - ts_event
    assert slo_delta < 10, f"Latency SLO violation: emit delay {slo_delta}ms >= 10ms"
    assert slo_delta >= 0, f"Latency contract violation: time traveling emit ({slo_delta}ms)"

    print("✅ Signal contract parity test passed!")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_signal_payload_quantity_parity())
