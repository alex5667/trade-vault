from __future__ import annotations

"""P8/P9/P10 tests: exec_health_freeze_integrity pure logic — dual-control + request-log aware."""

import os

from services.orderflow.exec_health_freeze_control import (
    build_autoguard_latch_update
    build_dual_control_commit_thaw_update
    sign_dual_control_commit
    sign_manual_ack
)
from services.orderflow.exec_health_freeze_integrity import evaluate_freeze_integrity

_SECRET = "test-signing-secret"
_NONCE = "cafebabe"
_NOW = 1_700_000_000_000
_TRIGGER_TS = _NOW - 60_000


def _latch(nonce: str = _NONCE) -> dict:
    return build_autoguard_latch_update(
        prev={}
        now_ms=_TRIGGER_TS
        reasons=["cross_scope_mode_mismatch"]
        freeze_until_ts_ms=_TRIGGER_TS + 1_800_000
        ack_nonce=nonce
        trigger_event_id="latch-event-1"
    )


def _trigger_stream_event(nonce: str = _NONCE) -> tuple:
    return ("latch-event-1", {
        "kind": "autoguard_freeze_latch"
        "ack_nonce": nonce
        "trigger_ts_ms": str(_TRIGGER_TS)
    })


# ─── P8 legacy signed ack ─────────────────────────────────────────────────────

def _p8_ack_event(nonce: str = _NONCE) -> tuple:
    sig = sign_manual_ack(
        secret=_SECRET
        action="thaw"
        operator="alice"
        reason="fix verified"
        ticket="INC-1"
        ack_nonce=nonce
        trigger_ts_ms=_TRIGGER_TS
        ack_ts_ms=_NOW + 5000
    )
    return ("p8-ack-1", {
        "kind": "manual_ack_thaw"
        "operator": "alice"
        "reason": "fix verified"
        "ticket": "INC-1"
        "ack_nonce": nonce
        "trigger_ts_ms": str(_TRIGGER_TS)
        "ts_ms": str(_NOW + 5000)
        "ack_sig": sig
    })


# ─── P9 dual-control commit event ─────────────────────────────────────────────

def _p9_commit_event(nonce: str = _NONCE, prepared_ts: int = _TRIGGER_TS + 1000
                     approved_ts: int = _TRIGGER_TS + 2000, commit_ts: int = _NOW + 5000) -> tuple:
    sig = sign_dual_control_commit(
        secret=_SECRET
        request_id="req-abc"
        ack_nonce=nonce
        prepared_by="alice"
        approved_by="bob"
        commit_by="alice"
        reason="fix verified"
        ticket="INC-2"
        trigger_ts_ms=_TRIGGER_TS
        prepared_ts_ms=prepared_ts
        approved_ts_ms=approved_ts
        commit_ts_ms=commit_ts
    )
    return ("p9-commit-1", {
        "kind": "manual_ack_thaw_commit"
        "request_id": "req-abc"
        "operator": "alice"
        "prepared_by": "alice"
        "approved_by": "bob"
        "reason": "fix verified"
        "ticket": "INC-2"
        "ack_nonce": nonce
        "trigger_ts_ms": str(_TRIGGER_TS)
        "prepared_ts_ms": str(prepared_ts)
        "approved_ts_ms": str(approved_ts)
        "ts_ms": str(commit_ts)
        "commit_sig": sig
    })


# ─── P8 tests ─────────────────────────────────────────────────────────────────

def test_missing_signed_ack_event_triggers_violation() -> None:
    """Нет ack событий в стриме → нарушение."""
    os.environ["EXEC_HEALTH_ACK_SIGNING_SECRET"] = _SECRET
    os.environ["EXEC_HEALTH_REQUIRE_DUAL_CONTROL_ACK"] = "0"
    control = _latch()
    res = evaluate_freeze_integrity(
        control_raw=control
        state_raw={}
        events=[_trigger_stream_event()]
        request_events=[]
        now_ms=_NOW + 1000
    )
    assert res.pending_ack_nonce == _NONCE
    assert not res.valid_ack_event_present
    assert "none" not in res.violation_kinds


def test_p8_valid_signed_ack_clears_violation() -> None:
    """P8 валидный ack снимает нарушение."""
    os.environ["EXEC_HEALTH_ACK_SIGNING_SECRET"] = _SECRET
    os.environ["EXEC_HEALTH_REQUIRE_DUAL_CONTROL_ACK"] = "0"
    control = _latch()
    res = evaluate_freeze_integrity(
        control_raw=control
        state_raw={}
        events=[_trigger_stream_event(), _p8_ack_event()]
        request_events=[]
        now_ms=_NOW + 10_000
    )
    assert res.valid_ack_event_present
    assert "none" in res.violation_kinds


# ─── P9 tests ─────────────────────────────────────────────────────────────────

def test_p9_dual_control_commit_event_clears_violation() -> None:
    """P9 dual-control commit event снимает нарушение."""
    os.environ["EXEC_HEALTH_ACK_SIGNING_SECRET"] = _SECRET
    os.environ["EXEC_HEALTH_REQUIRE_DUAL_CONTROL_ACK"] = "1"
    control = _latch()
    commit = _p9_commit_event()
    # P10: commit event now goes to request_events stream
    request_events = [
        ("1-0", {"kind": "manual_ack_thaw_prepare", "request_id": "req-abc", "ack_nonce": _NONCE, "operator": "alice"})
        ("2-0", {"kind": "manual_ack_thaw_approve", "request_id": "req-abc", "ack_nonce": _NONCE, "operator": "bob"})
        ("3-0", {**commit[1]}),  # commit event in request stream too
    ]
    res = evaluate_freeze_integrity(
        control_raw=control
        state_raw={}
        events=[_trigger_stream_event(), commit]
        request_events=request_events
        now_ms=_NOW + 10_000
    )
    assert res.valid_ack_event_present
    assert res.valid_ack_event_id in ("p9-commit-1", "3-0")
    assert "none" in res.violation_kinds


def test_p9_commit_event_with_invalid_sig_is_rejected() -> None:
    """P9: commit event с плохой подписью → invalid_ack_event_signature."""
    os.environ["EXEC_HEALTH_ACK_SIGNING_SECRET"] = _SECRET
    os.environ["EXEC_HEALTH_REQUIRE_DUAL_CONTROL_ACK"] = "1"
    control = _latch()
    bad_event = ("p9-bad-1", {
        "kind": "manual_ack_thaw_commit"
        "request_id": "req-abc"
        "operator": "mallory"
        "prepared_by": "alice"
        "approved_by": "mallory",  # same as prepared — would fail dual check
        "reason": "fake"
        "ticket": "T-0"
        "ack_nonce": _NONCE
        "trigger_ts_ms": str(_TRIGGER_TS)
        "prepared_ts_ms": "0"
        "approved_ts_ms": "0"
        "ts_ms": str(_NOW + 5000)
        "commit_sig": "badsig"
    })
    res = evaluate_freeze_integrity(
        control_raw=control
        state_raw={}
        events=[_trigger_stream_event(), bad_event]
        request_events=[]
        now_ms=_NOW + 10_000
    )
    assert not res.valid_ack_event_present
    assert "invalid_ack_event_signature" in res.violation_kinds


def test_p9_control_with_dual_control_commit_hash_no_violation() -> None:
    """P9: control hash содержит корректную dual-control commit подпись → нарушений нет."""
    os.environ["EXEC_HEALTH_ACK_SIGNING_SECRET"] = _SECRET
    os.environ["EXEC_HEALTH_REQUIRE_DUAL_CONTROL_ACK"] = "1"
    from services.orderflow.exec_health_freeze_control import build_thaw_approve_update, build_thaw_prepare_update
    latch = _latch()
    # Simulate real workflow: prepare → approve → commit (thaw_prepared_by/approved_by propagate)
    prepared = build_thaw_prepare_update(
        prev=latch, now_ms=_TRIGGER_TS + 1000, request_id="req-def"
        operator="alice", reason="ok", ticket="T-9"
        provided_ack_nonce=_NONCE
    )
    approved = build_thaw_approve_update(
        prev=prepared, now_ms=_TRIGGER_TS + 2000, request_id="req-def", approver="bob"
    )
    now_ms = _NOW + 10_000
    sig = sign_dual_control_commit(
        secret=_SECRET
        request_id="req-def"
        ack_nonce=_NONCE
        prepared_by="alice"
        approved_by="bob"
        commit_by="bob",   # P10: commit_by must equal approved_by
        reason="ok"
        ticket="T-9"
        trigger_ts_ms=_TRIGGER_TS
        prepared_ts_ms=_TRIGGER_TS + 1000
        approved_ts_ms=_TRIGGER_TS + 2000
        commit_ts_ms=now_ms
    )
    committed = build_dual_control_commit_thaw_update(
        prev=approved
        now_ms=now_ms
        request_id="req-def"
        commit_by="bob",   # approver commits
        commit_sig=sig
        commit_event_id="3-0"
    )
    # Also need a valid ack event in stream so integrity evaluator finds it
    commit_ev = _p9_commit_event(nonce=_NONCE, prepared_ts=_TRIGGER_TS + 1000
                                 approved_ts=_TRIGGER_TS + 2000, commit_ts=now_ms)
    # P10: build full request events with matching request_id=req-def
    # commit_by=bob == approved_by → valid P10 sequence
    request_events = [
        ("1-0", {"kind": "manual_ack_thaw_prepare", "request_id": "req-def", "ack_nonce": _NONCE, "operator": "alice"})
        ("2-0", {"kind": "manual_ack_thaw_approve", "request_id": "req-def", "ack_nonce": _NONCE, "operator": "bob"})
        ("3-0", {
            "kind": "manual_ack_thaw_commit"
            "request_id": "req-def"
            "operator": "bob",       # bob commits (= approved_by)
            "prepared_by": "alice"
            "approved_by": "bob"
            "reason": "ok"
            "ticket": "T-9"
            "ack_nonce": _NONCE
            "trigger_ts_ms": str(_TRIGGER_TS)
            "prepared_ts_ms": str(_TRIGGER_TS + 1000)
            "approved_ts_ms": str(_TRIGGER_TS + 2000)
            "ts_ms": str(now_ms)
            "commit_sig": sig
        })
    ]
    res = evaluate_freeze_integrity(
        control_raw=committed
        state_raw={}
        events=[_trigger_stream_event(), commit_ev]
        request_events=request_events
        now_ms=now_ms + 1000
    )
    # committed control has manual_override_action=thaw and valid dual-control sig
    # So no invalid_control_ack_signature and the event is valid
    assert "invalid_control_ack_signature" not in res.violation_kinds
    assert "none" in res.violation_kinds
    # P10: request log should be valid
    assert res.request_log_valid_sequence is True


# ─── P10 tests ────────────────────────────────────────────────────────────────

def test_p10_control_with_no_request_log_is_tamper() -> None:
    """P10: thaw in control hash but no backing request log → tamper violation."""
    os.environ["EXEC_HEALTH_ACK_SIGNING_SECRET"] = _SECRET
    os.environ["EXEC_HEALTH_REQUIRE_DUAL_CONTROL_ACK"] = "1"
    latch = _latch()
    control_with_thaw = {
        **latch
        "active_thaw_request_id": "req-tamper"
        "thaw_request_status": "committed"
        "thaw_prepared_by": "alice"
        "thaw_approved_by": "bob"
        "manual_commit_by": "bob"
        "manual_override_action": "thaw"
        "manual_override_active": "1"
    }
    res = evaluate_freeze_integrity(
        control_raw=control_with_thaw
        state_raw={}
        events=[_trigger_stream_event()]
        request_events=[],  # No backing log
        now_ms=_NOW + 10_000
    )
    assert res.request_log_valid_sequence is False
    assert "request_log_prepare_missing" in res.violation_kinds
