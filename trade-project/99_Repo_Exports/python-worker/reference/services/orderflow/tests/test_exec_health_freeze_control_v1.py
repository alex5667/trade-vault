from __future__ import annotations

import os

from services.orderflow.exec_health_freeze_control import (
    build_autoguard_latch_update,
    build_dual_control_commit_thaw_update,
    build_thaw_approve_update,
    build_thaw_prepare_update,
    parse_exec_health_freeze_control,
    sign_dual_control_commit,
)


def test_parse_control_autoguard_latch_freezes() -> None:
    raw = build_autoguard_latch_update(prev={}, now_ms=12_000, reasons=['drift'], freeze_until_ts_ms=40_000, ack_nonce='n1')
    st = parse_exec_health_freeze_control(raw, now_ms=15_000)
    assert st.effective_freeze_active is True
    assert st.expected_ack_nonce == 'n1'


def test_parse_control_dual_control_commit_clears_freeze() -> None:
    os.environ['EXEC_HEALTH_ACK_SIGNING_SECRET'] = 'test-secret'
    base = build_autoguard_latch_update(prev={}, now_ms=12_000, reasons=['drift'], freeze_until_ts_ms=40_000, ack_nonce='n1')
    prep = build_thaw_prepare_update(prev=base, now_ms=13_000, request_id='r1', operator='alice', reason='checked', ticket='INC-1', provided_ack_nonce='n1')
    appr = build_thaw_approve_update(prev=prep, now_ms=14_000, request_id='r1', approver='bob')
    sig = sign_dual_control_commit(secret='test-secret', request_id='r1', ack_nonce='n1', prepared_by='alice', approved_by='bob', commit_by='bob', reason='checked', ticket='INC-1', trigger_ts_ms=12_000, prepared_ts_ms=13_000, approved_ts_ms=14_000, commit_ts_ms=15_000)
    raw = build_dual_control_commit_thaw_update(prev=appr, now_ms=15_000, request_id='r1', commit_by='bob', commit_sig=sig, commit_event_id='5-0')
    st = parse_exec_health_freeze_control(raw, now_ms=16_000)
    assert st.effective_freeze_active is False
    assert st.manual_override_action == 'thaw'
    assert st.active_thaw_request_id == 'r1'
    assert st.thaw_approved_by == 'bob'


def test_same_operator_dual_control_commit_is_rejected_by_parser() -> None:
    os.environ['EXEC_HEALTH_ACK_SIGNING_SECRET'] = 'test-secret'
    base = build_autoguard_latch_update(prev={}, now_ms=12_000, reasons=['drift'], freeze_until_ts_ms=40_000, ack_nonce='n1')
    prep = build_thaw_prepare_update(prev=base, now_ms=13_000, request_id='r1', operator='alice', reason='checked', ticket='INC-1', provided_ack_nonce='n1')
    appr = build_thaw_approve_update(prev=prep, now_ms=14_000, request_id='r1', approver='alice')
    sig = sign_dual_control_commit(secret='test-secret', request_id='r1', ack_nonce='n1', prepared_by='alice', approved_by='alice', commit_by='alice', reason='checked', ticket='INC-1', trigger_ts_ms=12_000, prepared_ts_ms=13_000, approved_ts_ms=14_000, commit_ts_ms=15_000)
    raw = build_dual_control_commit_thaw_update(prev=appr, now_ms=15_000, request_id='r1', commit_by='alice', commit_sig=sig, commit_event_id='5-0')
    st = parse_exec_health_freeze_control(raw, now_ms=16_000)
    # Same operator: verify_dual_control_commit_signature returns False → stays frozen
    assert st.effective_freeze_active is True
