import asyncio
from unittest.mock import Mock, MagicMock, patch
from handlers.crypto_orderflow.pipeline.orchestrator import SignalOrchestrator, _normalize_ts_ms


def test_normalize_ts_ms():
    """_normalize_ts_ms returns 0 for anomalous inputs and increments ts_rejected_total."""
    now = 1_710_000_000_000  # 2024-03-10 ~UTC

    with patch(
        "handlers.crypto_orderflow.pipeline.orchestrator._TS_REJECTED_TOTAL"
    ) as mock_counter:
        mock_labels = MagicMock()
        mock_counter.labels.return_value = mock_labels

        # --- rejected inputs: must return 0 and increment counter ---
        assert _normalize_ts_ms(0, now, source="test") == 0
        mock_counter.labels.assert_called_with(source="test", reason="zero_or_negative")
        mock_labels.inc.assert_called()
        mock_counter.reset_mock()
        mock_labels.reset_mock()

        assert _normalize_ts_ms(-100, now, source="test") == 0
        mock_counter.labels.assert_called_with(source="test", reason="zero_or_negative")
        mock_labels.inc.assert_called()
        mock_counter.reset_mock()
        mock_labels.reset_mock()

        assert _normalize_ts_ms(2_000_000_000_000, now, source="test") == 0  # far future
        mock_counter.labels.assert_called_with(source="test", reason="future")
        mock_labels.inc.assert_called()
        mock_counter.reset_mock()
        mock_labels.reset_mock()

        assert _normalize_ts_ms("invalid", now, source="test") == 0
        mock_counter.labels.assert_called_with(source="test", reason="parse_error")
        mock_labels.inc.assert_called()
        mock_counter.reset_mock()
        mock_labels.reset_mock()

        # --- valid ms input: must return correct value and NOT increment counter ---
        result = _normalize_ts_ms(now, now, source="test")
        assert result == now
        mock_counter.labels.assert_not_called()

        # --- valid seconds input: must scale to ms ---
        sec_ts = now // 1000
        result = _normalize_ts_ms(sec_ts, now, source="test")
        assert result == sec_ts * 1000
        mock_counter.labels.assert_not_called()

    print("test_normalize_ts_ms: PASS")


def test_dlq_xadd_zero_ts_adds_dq_flag():
    """When ts=0, DLQ payload must contain dq_flags='ts_invalid'."""
    cfg = MagicMock()
    gates = MagicMock()
    liquidity = MagicMock()
    observability = MagicMock()
    confirmations = MagicMock()
    emitter = MagicMock()

    redis_mock = MagicMock()
    ctx = MagicMock()
    ctx.redis = redis_mock
    ctx.ts_ms = 0
    ctx.ts = 0
    ctx.symbol = "BTCUSDT"

    cand = MagicMock()
    cand.kind = "test_kind"

    orchestrator = SignalOrchestrator(cfg, gates, liquidity, observability, confirmations, emitter)

    gates.check_quality.return_value = MagicMock(veto=True, reason="VETO_TEST")

    with patch(
        "handlers.crypto_orderflow.pipeline.orchestrator._TS_REJECTED_TOTAL"
    ) as mock_counter:
        mock_counter.labels.return_value = MagicMock()
        orchestrator.process(ctx, lambda c: [cand])

    assert redis_mock.xadd.called, "xadd not called"
    args, kwargs = redis_mock.xadd.call_args
    assert args[0] == "stream:signals:dlq"
    payload = args[1]
    assert payload["ts_ms"] == "0", f"expected ts_ms='0', got {payload['ts_ms']!r}"
    assert payload.get("dq_flags") == "ts_invalid", (
        f"expected dq_flags='ts_invalid', got {payload.get('dq_flags')!r}"
    )
    print("test_dlq_xadd_zero_ts_adds_dq_flag: PASS")


def test_dlq_xadd_valid_ts_no_dq_flag():
    """When ts is valid, DLQ payload must NOT contain dq_flags='ts_invalid'."""
    import time

    cfg = MagicMock()
    gates = MagicMock()
    liquidity = MagicMock()
    observability = MagicMock()
    confirmations = MagicMock()
    emitter = MagicMock()

    redis_mock = MagicMock()
    ctx = MagicMock()
    ctx.redis = redis_mock
    ctx.ts_ms = int(time.time() * 1000)  # valid now_ms
    ctx.ts = None
    ctx.symbol = "ETHUSDT"

    cand = MagicMock()
    cand.kind = "good_kind"

    orchestrator = SignalOrchestrator(cfg, gates, liquidity, observability, confirmations, emitter)
    gates.check_quality.return_value = MagicMock(veto=True, reason="VETO_TEST")

    orchestrator.process(ctx, lambda c: [cand])

    assert redis_mock.xadd.called, "xadd not called"
    args, _ = redis_mock.xadd.call_args
    payload = args[1]
    assert payload.get("dq_flags", "") != "ts_invalid", (
        "dq_flags='ts_invalid' must not be set for valid ts"
    )
    print("test_dlq_xadd_valid_ts_no_dq_flag: PASS")


def test_edge_gate_event_xadd_exception():
    # Setup mocks
    cfg = MagicMock()
    gates = MagicMock()
    liquidity = MagicMock()
    observability = MagicMock()
    confirmations = MagicMock()
    emitter = MagicMock()

    redis_mock = MagicMock()
    # Emulate Redis timeout on xadd
    redis_mock.xadd.side_effect = Exception("Redis Timeout")
    ctx = MagicMock()
    ctx.redis = redis_mock

    cand = MagicMock()
    # We want it to pass all gates up to edge_cost
    gates.check_quality.return_value = MagicMock(veto=False)
    gates.check_regime_gate.return_value = (True, "")
    gates.check_smt.return_value = MagicMock(veto=False)
    gates.consistency_once.return_value = MagicMock(veto=False)
    cost_decision = MagicMock(veto=False, expected_move_bps=10.0, threshold_bps=5.0)
    gates.edge_cost_cached.return_value = cost_decision

    import os
    os.environ["EDGE_GATE_EVENTS_MODE"] = "on"
    os.environ["EDGE_GATE_SAMPLE_PASS"] = "1.0"

    orchestrator = SignalOrchestrator(cfg, gates, liquidity, observability, confirmations, emitter)
    # mock _emit_build_failed to verify it is NOT called
    orchestrator._emit_build_failed = MagicMock()

    orchestrator.process(ctx, lambda c: [cand])

    # Verify _emit_build_failed was NOT called
    assert not orchestrator._emit_build_failed.called, "_emit_build_failed was called!"
    print("test_edge_gate_event_xadd_exception: PASS")


if __name__ == "__main__":
    test_normalize_ts_ms()
    test_dlq_xadd_zero_ts_adds_dq_flag()
    test_dlq_xadd_valid_ts_no_dq_flag()
    test_edge_gate_event_xadd_exception()
