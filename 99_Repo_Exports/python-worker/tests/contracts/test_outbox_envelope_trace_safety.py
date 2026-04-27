from __future__ import annotations

from types import SimpleNamespace
import pytest

from services.outbox.envelope_builder import build_outbox_envelope
from common.contracts.json_contract import assert_json_safe, assert_no_trace_in_tradeable_envelope

def test_build_outbox_envelope_trace_safe():
    # Test that envelope is trade-safe even when trace data is provided
    env = build_outbox_envelope(
        sid="SID123",
        ctx=None,
        kind="breakout",
        symbol="BTCUSDT",
        notify_payload={"text": "hi"},
        meta={"x": 1},
        trace={"trace_id": "T1", "events": [{"type": "gate"}]},
    )

    assert_json_safe(env)
    assert_no_trace_in_tradeable_envelope(env)

    assert env.get("trace_id") == "T1"
    assert isinstance(env.get("trace_summary"), str) and env["trace_summary"]
    assert "meta" in env and isinstance(env["meta"], dict)
    # trace_meta_key may not be set for direct trace parameter (backward compatibility)
    # but envelope is still trade-safe
