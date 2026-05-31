"""F7 regression: SignalDispatcher (signal_outbox_dispatcher.py) must
auto-repair flat-shape envelopes the same way EnvelopeParser does, so both
dispatchers (signal_outbox_dispatcher and signal_dispatcher) behave
symmetrically when handed legacy producer output.
"""
from __future__ import annotations

import json

from services.signal_outbox_dispatcher import SignalDispatcher


class _FakeDispatcher:
    """Slice of SignalDispatcher needed to call _parse_envelope() directly."""
    _parse_envelope = SignalDispatcher._parse_envelope


def test_canonical_data_field_passes_through():
    d = _FakeDispatcher()
    env = {
        "sid": "s1",
        "ts_ms": 123,
        "symbol": "BTCUSDT",
        "schema_version": 1,
        "targets": {"audit_payload": {"x": 1}},
        "meta": {"audit_stream": "stream:audit"},
    }
    out = d._parse_envelope({"data": json.dumps(env)})
    assert out is not None
    assert out["sid"] == "s1"
    assert out["targets"]["audit_payload"] == {"x": 1}


def test_flat_envelope_is_auto_repaired_into_targets_meta():
    d = _FakeDispatcher()
    flat = {
        "sid": "s2",
        "ts_ms": 456,
        "symbol": "ETHUSDT",
        "schema_version": 1,
        "audit_payload": {"audit": True},
        "notify_payload": {"text": "hi"},
        "signal_stream_payload": {"score": 0.7},
        "audit_stream": "stream:audit",
        "signal_stream": "stream:signal",
    }
    out = d._parse_envelope({"payload_json": json.dumps(flat)})
    assert out is not None
    assert out["targets"]["audit_payload"] == {"audit": True}
    assert out["targets"]["notify"] == {"text": "hi"}
    assert out["targets"]["signal_stream_payload"] == {"score": 0.7}
    assert out["meta"]["audit_stream"] == "stream:audit"
    assert out["meta"]["signal_stream"] == "stream:signal"
    # Originals lifted, not duplicated:
    assert "audit_payload" not in out
    assert "notify_payload" not in out
    assert "signal_stream" not in out


def test_flat_envelope_lifts_signal_id_to_sid():
    d = _FakeDispatcher()
    flat = {
        "signal_id": "legacy-sid",
        "audit_payload": {"k": "v"},
    }
    out = d._parse_envelope({"payload_json": json.dumps(flat)})
    assert out is not None
    assert out["sid"] == "legacy-sid"


def test_unknown_shape_returns_none():
    d = _FakeDispatcher()
    assert d._parse_envelope({}) is None
    assert d._parse_envelope({"data": ""}) is None
    assert d._parse_envelope({"data": "not-json"}) is None


def test_bytes_data_decoded():
    d = _FakeDispatcher()
    env = {"sid": "b1", "targets": {}, "meta": {}}
    out = d._parse_envelope({"data": json.dumps(env).encode("utf-8")})
    assert out is not None
    assert out["sid"] == "b1"
