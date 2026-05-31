from __future__ import annotations

from typing import Any

from common.contracts.json_contract import assert_json_safe


def assert_tradeable_payload_contract(payload: dict[str, Any]) -> None:
    assert_json_safe(payload)
    # запрет на утечки диагностики внутрь tradeable payload
    if "trace" in payload or "events" in payload:
        raise AssertionError("payload leaked trace/events")
    if "parts_full" in payload:
        raise AssertionError("payload must not contain parts_full (heavy diagnostics)")
