from __future__ import annotations

import json

from services.orderflow.exec_health_freeze_hook import (
    build_exec_health_auto_freeze_decision
    parse_exec_health_auto_freeze
)


def test_entry_reason_code_variant_for_auto_freeze() -> None:
    """Entry path uses DENY_EXEC_HEALTH_AUTO_FREEZE (not VETO_) reason code."""
    raw = json.dumps({
        "freeze_active": 1
        "freeze_reason": "cross_scope_mode_mismatch"
        "freeze_until_ts_ms": 50_000
        "ts_ms": 10_000
    })
    st = parse_exec_health_auto_freeze(raw, now_ms=20_000)
    dec = build_exec_health_auto_freeze_decision(
        scope="entry_policy"
        state=st
        reason_code="DENY_EXEC_HEALTH_AUTO_FREEZE"
    )
    assert dec.block is True
    assert dec.reason_code == "DENY_EXEC_HEALTH_AUTO_FREEZE"
    assert "scope=entry_policy" in dec.notes
    assert "freeze_until_ts_ms=50000" in dec.notes
