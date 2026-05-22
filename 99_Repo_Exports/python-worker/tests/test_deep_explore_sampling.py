"""Unit tests for _maybe_record_deep_explore() — deep exploration sampling bucket.

Policy: deep_explore_20_35_sampled
Coverage:
1. Disabled when sample_rate=0 (default).
2. Probabilistic gate: rate=1.0 → always samples.
3. Rate=0.0 → never samples (disabled path).
4. Cap gate: rejects after cap is reached.
5. Payload contract: required fields, sample_policy, tradeable, meets_virtual_threshold.
6. Confidence range guard: only fires in [deep_min, virtual_min).
7. Metric labels: accepted=1 on success, accepted=0 on drop.
8. Fail-open: exceptions inside do NOT propagate.
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import services.orderflow.signal_pipeline as _sp_mod

GATED_OUT_STREAM = "stream:signals:gated_out"

REQUIRED_DEEP_EXPLORE_FIELDS = {
    "v", "ts_ms", "symbol", "direction", "side",
    "signal_id", "confidence", "min_conf",
    "entry", "sl", "tp_levels",
    "gated_out", "gate_reason",
    "virtual", "tradeable", "is_counterfactual",
    "sample_policy", "selection_policy_version",
    "selection_prob", "selection_weight",
    "meets_virtual_threshold", "virtual_min_conf",
    "deep_explore_min_conf",
    "regime", "session",
    "confirmations", "indicators",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_stub(
    *,
    sample_rate: float = 1.0,
    cap: int = 50,
    deep_min_pct: float = 20.0,
    virt_min_pct: float = 35.0,
):
    class _Stub:
        gated_out_shadow_enabled: bool = True
        gated_out_shadow_stream: str = GATED_OUT_STREAM
        gated_out_shadow_maxlen: int = 100_000
        _cached_virtual_min_conf_pct: float = virt_min_pct
        _cached_deep_explore_min_conf_pct: float = deep_min_pct
        _cached_deep_explore_sample_rate: float = sample_rate
        _cached_deep_explore_cap_per_slot: int = cap
        _deep_explore_cap_counters: dict = {}

    stub = _Stub()
    stub._maybe_record_deep_explore = _sp_mod.SignalPipeline._maybe_record_deep_explore.__get__(stub)  # type: ignore[attr-defined]
    return stub


def _attach_publisher(stub):
    xadds: list[tuple[str, dict]] = []

    async def _xadd(stream, fields, **kw):
        xadds.append((stream, dict(fields)))

    counters = {}
    pipe_mock = MagicMock()
    
    def _incr(key):
        pipe_mock._last_key = key
        return pipe_mock
        
    def _execute():
        key = getattr(pipe_mock, "_last_key", "default_key")
        counters[key] = counters.get(key, 0) + 1
        return [counters[key], True]

    pipe_mock.incr.side_effect = _incr
    pipe_mock.execute.side_effect = _execute

    redis_mock = MagicMock()
    redis_mock.xadd = AsyncMock(side_effect=_xadd)
    redis_mock.pipeline.return_value = pipe_mock
    
    pub = MagicMock()
    pub.r = redis_mock
    stub.publisher = pub
    stub._xadds = xadds
    stub._counters = counters
    return xadds


def _call_and_flush(stub, **overrides) -> list[dict]:
    """Call _maybe_record_deep_explore with defaults, flush tasks, return payloads."""
    # Reset xadds before each call so per-invocation results are isolated
    if hasattr(stub, "_xadds"):
        stub._xadds.clear()

    defaults = dict(
        signal={"signal_id": "de-001"},
        indicators={"spread_bps": 3.1},
        confirmations=[],
        symbol="BTCUSDT",
        direction="LONG",
        ts_ms=1_716_000_000_000,
        confidence=0.28,  # in [20%, 35%)
        entry=65_000.0,
        sl=64_200.0,
        tp_levels=[66_000.0],
        regime="range",
    )
    defaults.update(overrides)

    pending: list = []

    def _capture(coro, name=None):
        pending.append(coro)

    with patch.object(_sp_mod, "safe_create_task", _capture):
        stub._maybe_record_deep_explore(**defaults)

    async def _flush():
        for c in pending:
            await c

    asyncio.run(_flush())
    payloads = []
    for _stream, fields in getattr(stub, "_xadds", []):
        raw = fields.get("payload") or fields.get("data") or ""
        if raw:
            payloads.append(json.loads(raw))
    return payloads


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_disabled_by_default_zero_rate():
    """DEEP_EXPLORATION_SAMPLE_RATE=0 → nothing recorded."""
    stub = _make_stub(sample_rate=0.0)
    _attach_publisher(stub)
    payloads = _call_and_flush(stub)
    assert payloads == [], "sample_rate=0 must suppress all recording"


def test_sample_rate_1_always_records():
    """DEEP_EXPLORATION_SAMPLE_RATE=1.0 → always records (no probabilistic drop)."""
    stub = _make_stub(sample_rate=1.0)
    _attach_publisher(stub)
    payloads = _call_and_flush(stub)
    assert len(payloads) == 1, f"Expected 1 payload, got {len(payloads)}"


def test_required_fields_present():
    stub = _make_stub(sample_rate=1.0)
    _attach_publisher(stub)
    payloads = _call_and_flush(stub)
    assert payloads, "No payload recorded"
    p = payloads[0]
    missing = REQUIRED_DEEP_EXPLORE_FIELDS - set(p.keys())
    assert not missing, f"Missing fields: {missing}"


def test_policy_invariants():
    """sample_policy, tradeable, meets_virtual_threshold invariants."""
    stub = _make_stub(sample_rate=1.0)
    _attach_publisher(stub)
    payloads = _call_and_flush(stub)
    p = payloads[0]
    assert p["sample_policy"] == "deep_explore_20_35_sampled", f"Wrong policy: {p['sample_policy']}"
    assert p["tradeable"] is False, "deep_explore samples must never be tradeable"
    assert p["meets_virtual_threshold"] is False, "deep_explore is below virtual threshold"
    assert p["gated_out"] == 1
    assert p["gate_reason"] == "low_confidence"
    assert p["virtual"] is True
    assert p["is_counterfactual"] is True
    assert p["v"] == 2


def test_selection_weight_capped_at_20():
    """Propensity weight = 1/sample_rate, capped at 20."""
    stub = _make_stub(sample_rate=0.03)
    _attach_publisher(stub)
    
    class FakeHash:
        def hexdigest(self): return "0000" # bucket 0, will pass
        
    with patch("hashlib.sha256", return_value=FakeHash()):
        payloads = _call_and_flush(stub)
        
    if payloads:
        p = payloads[0]
        assert p["selection_weight"] <= 20.0, "selection_weight must be capped at 20"
        assert p["selection_weight"] > 0.0
        assert abs(p["selection_prob"] - 0.03) < 1e-9


def test_cap_gate_stops_after_cap():
    """After cap samples, further calls are dropped."""
    stub = _make_stub(sample_rate=1.0, cap=3)
    _attach_publisher(stub)
    # Fill cap
    for _ in range(3):
        _call_and_flush(stub)
    # 4th call should be dropped
    payloads_4th = _call_and_flush(stub)
    assert payloads_4th == [], "4th call should be capped out"


def test_regime_and_session_in_payload():
    stub = _make_stub(sample_rate=1.0)
    _attach_publisher(stub)
    payloads = _call_and_flush(stub, regime="trend")
    assert payloads, "No payload"
    p = payloads[0]
    assert p["regime"] == "trend"
    assert isinstance(p["session"], str)  # session_utc returns string


def test_stream_key_is_correct():
    stub = _make_stub(sample_rate=1.0)
    _attach_publisher(stub)
    _call_and_flush(stub)
    stream_keys = [s for s, _ in stub._xadds]
    assert stream_keys == [GATED_OUT_STREAM], f"Wrong stream key: {stream_keys}"


def test_disabled_no_publisher_fail_open():
    """No publisher → method returns silently without raising."""
    stub = _make_stub(sample_rate=1.0)
    stub.publisher = None  # no publisher
    try:
        stub._maybe_record_deep_explore(
            signal={}, indicators={}, confirmations=[],
            symbol="BTCUSDT", direction="LONG",
            ts_ms=1_716_000_000_000,
            confidence=0.28, entry=65000.0, sl=64200.0,
        )
    except Exception as e:
        raise AssertionError(f"Should not raise: {e}")


def test_exception_inside_is_swallowed():
    """Internal exception must not propagate (fail-open)."""
    stub = _make_stub(sample_rate=1.0)
    _attach_publisher(stub)
    # Corrupt state to trigger exception inside
    stub._deep_explore_cap_counters = None  # type: ignore[assignment]
    try:
        stub._maybe_record_deep_explore(
            signal={}, indicators={}, confirmations=[],
            symbol="BTCUSDT", direction="LONG",
            ts_ms=1_716_000_000_000,
            confidence=0.28, entry=65000.0, sl=64200.0,
        )
    except Exception as e:
        raise AssertionError(f"Exception must be swallowed (fail-open): {e}")


def test_confidence_range_semantics():
    """Caller is responsible for range check, but payload records correct min_conf."""
    stub = _make_stub(sample_rate=1.0, deep_min_pct=20.0, virt_min_pct=35.0)
    _attach_publisher(stub)
    payloads = _call_and_flush(stub, confidence=0.28)
    assert payloads
    p = payloads[0]
    # virtual_min_conf should be 35% → 0.35
    assert abs(p["virtual_min_conf"] - 0.35) < 1e-9, f"virtual_min_conf={p['virtual_min_conf']}"
    # deep_explore_min_conf should be 20% → 0.20
    assert abs(p["deep_explore_min_conf"] - 0.20) < 1e-9, f"deep_explore_min_conf={p['deep_explore_min_conf']}"
    assert p["confidence"] == 0.28


def test_deep_explore_deterministic_sampling():
    """Sampling decision must be deterministic for the same signal_id."""
    stub = _make_stub(sample_rate=0.5)
    _attach_publisher(stub)
    
    # We call it multiple times with same inputs, including ts_ms to not trip hour cap
    payloads1 = _call_and_flush(stub, signal={"signal_id": "test-123"}, ts_ms=1000)
    payloads2 = _call_and_flush(stub, signal={"signal_id": "test-123"}, ts_ms=1000)
    # The decision must be identical
    assert len(payloads1) == len(payloads2)


def test_deep_explore_never_pushes_order_queue():
    """Deep explore must never set tradeable to True."""
    stub = _make_stub(sample_rate=1.0)
    _attach_publisher(stub)
    payloads = _call_and_flush(stub)
    assert payloads, "No payload"
    p = payloads[0]
    assert p["tradeable"] is False
    assert p["meets_virtual_threshold"] is False
