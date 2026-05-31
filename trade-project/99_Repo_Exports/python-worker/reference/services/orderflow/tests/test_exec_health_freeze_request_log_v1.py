from __future__ import annotations

"""P10 tests: append-only request log sequence evaluation."""

import os

from services.orderflow.exec_health_freeze_control import (
    build_autoguard_latch_update,
    build_thaw_approve_update,
    build_thaw_prepare_update,
    sign_dual_control_commit,
)
from services.orderflow.exec_health_freeze_request_log import evaluate_request_log_sequence

_SECRET = "test-signing-secret"
_NONCE = "cafebabe"
_NOW = 1_700_000_000_000
_TRIGGER_TS = _NOW - 60_000


def _latch(nonce: str = _NONCE) -> dict:
    return build_autoguard_latch_update(
        prev={},
        now_ms=_TRIGGER_TS,
        reasons=["cross_scope_mode_mismatch"],
        freeze_until_ts_ms=_TRIGGER_TS + 1_800_000,
        ack_nonce=nonce,
        trigger_event_id="latch-event-1",
    )


def _prepare_event(request_id: str, nonce: str = _NONCE, operator: str = "alice") -> tuple:
    return ("1-0", {
        "kind": "manual_ack_thaw_prepare",
        "request_id": request_id,
        "ack_nonce": nonce,
        "operator": operator,
    })


def _approve_event(request_id: str, nonce: str = _NONCE, operator: str = "bob") -> tuple:
    return ("2-0", {
        "kind": "manual_ack_thaw_approve",
        "request_id": request_id,
        "ack_nonce": nonce,
        "operator": operator,
    })


def _commit_event(request_id: str, nonce: str = _NONCE, approved_by: str = "bob", commit_by: str = "bob") -> tuple:
    sig = sign_dual_control_commit(
        secret=_SECRET,
        request_id=request_id,
        ack_nonce=nonce,
        prepared_by="alice",
        approved_by=approved_by,
        commit_by=commit_by,
        reason="fix verified",
        ticket="INC-1",
        trigger_ts_ms=_TRIGGER_TS,
        prepared_ts_ms=_TRIGGER_TS + 1000,
        approved_ts_ms=_TRIGGER_TS + 2000,
        commit_ts_ms=_NOW,
    )
    return ("3-0", {
        "kind": "manual_ack_thaw_commit",
        "request_id": request_id,
        "ack_nonce": nonce,
        "operator": commit_by,
        "prepared_by": "alice",
        "approved_by": approved_by,
        "reason": "fix verified",
        "ticket": "INC-1",
        "trigger_ts_ms": str(_TRIGGER_TS),
        "prepared_ts_ms": str(_TRIGGER_TS + 1000),
        "approved_ts_ms": str(_TRIGGER_TS + 2000),
        "ts_ms": str(_NOW),
        "commit_sig": sig,
    })


def test_valid_request_log_sequence() -> None:
    """P10: full prepare→approve→commit sequence with distinct operators → valid."""
    os.environ["EXEC_HEALTH_ACK_SIGNING_SECRET"] = _SECRET
    request_id = "req-p10-valid"
    latch = _latch()
    prepared = build_thaw_prepare_update(
        prev=latch, now_ms=_TRIGGER_TS + 1000, request_id=request_id,
        operator="alice", reason="ok", ticket="T-10", provided_ack_nonce=_NONCE,
    )
    approved = build_thaw_approve_update(
        prev=prepared, now_ms=_TRIGGER_TS + 2000, request_id=request_id, approver="bob"
    )
    request_events = [
        _prepare_event(request_id, _NONCE, "alice"),
        _approve_event(request_id, _NONCE, "bob"),
        _commit_event(request_id, _NONCE, "bob", "bob"),
    ]
    res = evaluate_request_log_sequence(
        control_raw=approved,
        request_events=request_events,
        request_id=request_id,
    )
    assert res.valid_sequence is True
    assert res.prepare_present is True
    assert res.approve_present is True
    assert res.commit_present is True
    assert res.same_operator_violation is False
    assert "none" in res.violation_kinds


def test_control_commit_without_request_log_is_tamper() -> None:
    """P10: thaw in control hash with no backing request log events → violations."""
    os.environ["EXEC_HEALTH_ACK_SIGNING_SECRET"] = _SECRET
    request_id = "req-p10-tamper"
    latch = _latch()
    # Simulate a control hash that says 'committed' but no request events exist
    committed_control = {
        **latch,
        "active_thaw_request_id": request_id,
        "thaw_request_status": "committed",
        "thaw_prepared_by": "alice",
        "thaw_approved_by": "bob",
        "manual_commit_by": "bob",
        "manual_override_action": "thaw",
    }
    res = evaluate_request_log_sequence(
        control_raw=committed_control,
        request_events=[],  # empty log — no backing events
        request_id=request_id,
    )
    assert res.valid_sequence is False
    assert res.prepare_present is False
    assert "request_log_prepare_missing" in res.violation_kinds


def test_same_operator_violation() -> None:
    """P10: same operator prepares and approves → same_operator_violation."""
    request_id = "req-same-op"
    latch = _latch()
    prepared = build_thaw_prepare_update(
        prev=latch, now_ms=_TRIGGER_TS + 1000, request_id=request_id,
        operator="alice", reason="ok", ticket="T-11", provided_ack_nonce=_NONCE,
    )
    request_events = [
        _prepare_event(request_id, _NONCE, "alice"),
        _approve_event(request_id, _NONCE, "alice"),  # same operator!
    ]
    res = evaluate_request_log_sequence(
        control_raw=prepared,
        request_events=request_events,
        request_id=request_id,
    )
    assert res.same_operator_violation is True
    assert res.valid_sequence is False
    assert "request_log_same_operator_violation" in res.violation_kinds


def test_missing_approve_event() -> None:
    """P10: prepare present but approve missing → request_log_approve_missing."""
    request_id = "req-no-approve"
    latch = _latch()
    request_events = [
        _prepare_event(request_id, _NONCE, "alice"),
    ]
    res = evaluate_request_log_sequence(
        control_raw=latch,
        request_events=request_events,
        request_id=request_id,
    )
    assert res.approve_present is False
    assert res.valid_sequence is False
    assert "request_log_approve_missing" in res.violation_kinds


def test_empty_request_id_no_violations() -> None:
    """P10: when there's no active request_id, no request-log violations are raised."""
    latch = _latch()
    res = evaluate_request_log_sequence(
        control_raw=latch,
        request_events=[],
        request_id=None,
    )
    # No request_id set in latch → no violations expected from request log
    assert res.valid_sequence is False
    assert "none" in res.violation_kinds
