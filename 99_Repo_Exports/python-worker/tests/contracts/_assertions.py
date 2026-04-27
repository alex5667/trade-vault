from __future__ import annotations
from typing import Any, Dict
from common.contracts.json_contract import assert_json_safe, assert_no_trace_in_tradeable_envelope

def assert_tradeable_payload_contract(payload: Dict[str, Any]) -> None:
    assert_json_safe(payload)
    # запрет на утечки диагностики внутрь tradeable payload
    if "trace" in payload or "events" in payload:
        raise AssertionError("payload leaked trace/events")
    if "parts_full" in payload:
        raise AssertionError("payload must not contain parts_full (heavy diagnostics)")
