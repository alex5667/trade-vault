"""Contract test for signals:gated_out (shadow stream for confidence-gated-out signals).

Schema version: v=2 (int field, not schema_version).
  v=2 adds ML training metadata: virtual, tradeable, is_counterfactual,
  sample_policy, selection_policy_version, selection_prob, selection_weight,
  meets_virtual_threshold, virtual_min_conf.
Stream key: RS.SIGNAL_GATED_OUT ("stream:signals:gated_out")
Wire format: {"payload": JSON-string}

Verifies:
1. Required fields present and correctly typed.
2. gated_out == 1, gate_reason == "low_confidence" (producer invariant).
3. confidence < min_conf (only gated-out signals are recorded here).
4. indicators and confirmations are JSON-serializable containers.
5. tp_levels is always a list (never None in wire payload).
6. Disabled flag suppresses publish.
7. Stream key is correct.
8. ML metadata fields present (v=2).
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import services.orderflow.signal_pipeline as _sp_mod


REQUIRED_FIELDS = {
    "v", "ts_ms", "symbol", "direction", "side",
    "signal_id", "confidence", "min_conf",
    "entry", "sl", "tp_levels",
    "gated_out", "gate_reason",
    "confirmations", "indicators",
}

GATED_OUT_STREAM = "stream:signals:gated_out"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_stub():
    class _Stub:
        gated_out_shadow_enabled: bool = True
        gated_out_shadow_stream: str = GATED_OUT_STREAM
        gated_out_shadow_maxlen: int = 100_000
        _cached_virtual_min_conf_pct: float = 35.0  # separate virtual threshold (plan §5)
        # deep_explore attrs needed so _record_gated_out_shadow doesn't AttributeError
        _cached_deep_explore_min_conf_pct: float = 20.0
        _cached_deep_explore_sample_rate: float = 0.0
        _cached_deep_explore_cap_per_slot: int = 50
        _deep_explore_cap_counters: dict = {}

    stub = _Stub()
    stub._record_gated_out_shadow = _sp_mod.SignalPipeline._record_gated_out_shadow.__get__(stub)
    return stub


def _attach_publisher(stub):
    """Attach a publisher with a redis mock that captures xadd calls."""
    xadds: list[tuple[str, dict]] = []

    async def _xadd(stream, fields, **kw):
        xadds.append((stream, dict(fields)))

    redis_mock = MagicMock()
    redis_mock.xadd = AsyncMock(side_effect=_xadd)
    pub = MagicMock()
    pub.r = redis_mock
    stub.publisher = pub
    return xadds


def _call_and_flush(stub, **overrides) -> list[dict]:
    """Call _record_gated_out_shadow, flush pending safe_create_task coroutines, return payloads."""
    defaults = dict(
        signal={"signal_id": "gout-001"},
        indicators={"spread_bps": 4.2},
        confirmations=["rsi_agree=1"],
        symbol="BTCUSDT",
        direction="LONG",
        ts_ms=1_716_000_000_000,
        confidence=0.45,
        min_conf=0.60,
        entry=65_000.0,
        sl=64_200.0,
        tp_levels=[66_000.0, 67_000.0],
    )
    defaults.update(overrides)

    pending: list = []

    def _capture(coro, name=None):
        pending.append(coro)

    with patch.object(_sp_mod, "safe_create_task", _capture):
        stub._record_gated_out_shadow(**defaults)

    async def _flush():
        for c in pending:
            await c

    asyncio.run(_flush())

    xadds = getattr(stub, "_xadds", [])
    payloads = []
    for _stream, fields in xadds:
        raw = fields.get("payload") or fields.get("data") or ""
        if raw:
            payloads.append(json.loads(raw))
    return payloads


def _make_full_stub(**overrides):
    stub = _make_stub()
    xadds = _attach_publisher(stub)

    async def _xadd(stream, fields, **kw):
        xadds.append((stream, dict(fields)))

    stub.publisher.r.xadd = AsyncMock(side_effect=_xadd)
    stub._xadds = xadds
    return stub


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_required_fields_present():
    stub = _make_full_stub()
    payloads = _call_and_flush(stub)
    assert len(payloads) == 1, f"Expected 1 payload, got {len(payloads)}"
    missing = REQUIRED_FIELDS - set(payloads[0].keys())
    assert not missing, f"Missing required fields: {missing}"


ML_METADATA_FIELDS = {
    "virtual", "tradeable", "is_counterfactual",
    "sample_policy", "selection_policy_version",
    "selection_prob", "selection_weight",
    "meets_virtual_threshold", "virtual_min_conf",
    # v2 additions: regime/session for per-policy Prometheus labeling
    "regime", "session",
}


def test_gated_out_invariants():
    stub = _make_full_stub()
    payloads = _call_and_flush(stub)
    p = payloads[0]
    assert p["v"] == 2, "schema version must be 2 (v2 adds ML training metadata)"
    assert p["gated_out"] == 1, "gated_out must be 1"
    assert p["gate_reason"] == "low_confidence", f"gate_reason={p['gate_reason']!r}"
    assert p["confidence"] < p["min_conf"], "confidence must be below min_conf"
    assert p["virtual"] is True, "gated_out signals are always virtual"
    assert p["tradeable"] is False, "gated_out signals are never tradeable"
    assert p["is_counterfactual"] is True
    assert p["sample_policy"] == "confidence_gated_out"
    missing_ml = ML_METADATA_FIELDS - set(p.keys())
    assert not missing_ml, f"Missing ML metadata fields: {missing_ml}"


def test_direction_and_side_encoding():
    stub = _make_full_stub()
    payloads = _call_and_flush(stub, direction="SHORT")
    p = payloads[0]
    assert p["direction"] == "SHORT"
    assert p["side"] == "short", "side must be direction.lower()"


def test_tp_levels_none_becomes_empty_list():
    stub = _make_full_stub()
    payloads = _call_and_flush(stub, tp_levels=None)
    p = payloads[0]
    assert isinstance(p["tp_levels"], list), "tp_levels must be list"
    assert p["tp_levels"] == [], "tp_levels=None → []"


def test_indicators_json_serializable():
    stub = _make_full_stub()
    payloads = _call_and_flush(stub, indicators={"z": 2.5, "flag": True})
    p = payloads[0]
    assert isinstance(p["indicators"], dict)
    assert p["indicators"]["z"] == 2.5


def test_disabled_flag_skips_publish():
    stub = _make_full_stub()
    stub.gated_out_shadow_enabled = False
    payloads = _call_and_flush(stub)
    assert payloads == [], "disabled flag must suppress publish"


def test_stream_key_is_correct():
    stub = _make_full_stub()
    _call_and_flush(stub)
    stream_keys = [s for s, _ in stub._xadds]
    assert stream_keys == [GATED_OUT_STREAM], f"Wrong stream key: {stream_keys}"


def test_wire_format_uses_payload_key():
    stub = _make_full_stub()
    _call_and_flush(stub)
    assert stub._xadds, "Expected at least one xadd"
    _stream, fields = stub._xadds[0]
    assert "payload" in fields, f"Wire format must use 'payload' key; got keys: {list(fields.keys())}"
    # Payload value must be valid JSON
    json.loads(fields["payload"])
