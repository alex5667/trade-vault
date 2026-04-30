from __future__ import annotations

import os

from services.orderflow.exec_health_freeze_control import sign_dual_control_commit
from services.orderflow.exec_health_freeze_dual_control import evaluate_freeze_dual_control


def test_dual_control_evaluator_accepts_prepare_approve_commit_chain() -> None:
    os.environ['EXEC_HEALTH_ACK_SIGNING_SECRET'] = 'test-secret'
    control = {
        'active_thaw_request_id': 'r1'
        'thaw_request_status': 'committed'
        'thaw_request_nonce': 'n1'
        'thaw_prepared_by': 'alice'
        'thaw_approved_by': 'bob'
        'thaw_request_reason': 'validated'
        'thaw_request_ticket': 'INC-1'
        'thaw_prepare_ts_ms': '10500'
        'thaw_approve_ts_ms': '10800'
        'manual_override_action': 'thaw'
        'manual_override_active': '1'
        'manual_commit_request_id': 'r1'
        'manual_commit_by': 'bob'
        'manual_commit_ts_ms': '11000'
        'manual_commit_sig': sign_dual_control_commit(secret='test-secret', request_id='r1', ack_nonce='n1', prepared_by='alice', approved_by='bob', commit_by='bob', reason='validated', ticket='INC-1', trigger_ts_ms=10000, prepared_ts_ms=10500, approved_ts_ms=10800, commit_ts_ms=11000)
        'manual_ack_ts_ms': '11000'
        'manual_ack_nonce': 'n1'
        'last_trigger_ts_ms': '10000'
    }
    events = [
        ('5-0', {'kind': 'manual_ack_thaw_commit', 'request_id': 'r1', 'ack_nonce': 'n1', 'trigger_ts_ms': '10000', 'prepared_ts_ms': '10500', 'approved_ts_ms': '10800', 'ts_ms': '11000', 'operator': 'bob', 'prepared_by': 'alice', 'approved_by': 'bob', 'reason': 'validated', 'ticket': 'INC-1', 'commit_sig': control['manual_commit_sig']})
        ('4-0', {'kind': 'manual_ack_thaw_approve', 'request_id': 'r1', 'ack_nonce': 'n1', 'operator': 'bob'})
        ('3-0', {'kind': 'manual_ack_thaw_prepare', 'request_id': 'r1', 'ack_nonce': 'n1', 'operator': 'alice'})
    ]
    res = evaluate_freeze_dual_control(control_raw=control, state_raw={}, events=events)
    assert res.valid_commit_event_present is True
    assert res.violation_kinds == ['none']


def test_dual_control_evaluator_detects_same_operator_violation() -> None:
    res = evaluate_freeze_dual_control(
        control_raw={'active_thaw_request_id': 'r1', 'thaw_request_status': 'approved', 'thaw_prepared_by': 'alice', 'thaw_approved_by': 'alice'}
        state_raw={}
        events=[('4-0', {'kind': 'manual_ack_thaw_approve', 'request_id': 'r1', 'operator': 'alice'}), ('3-0', {'kind': 'manual_ack_thaw_prepare', 'request_id': 'r1', 'operator': 'alice'})]
    )
    assert 'same_operator_dual_control_violation' in res.violation_kinds
