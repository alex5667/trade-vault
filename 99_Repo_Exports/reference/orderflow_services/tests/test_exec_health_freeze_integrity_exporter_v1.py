from __future__ import annotations

import os

from services.orderflow.exec_health_freeze_control import sign_dual_control_commit
from services.orderflow.exec_health_freeze_integrity import evaluate_freeze_integrity


def test_integrity_evaluator_sees_valid_dual_control_commit_event() -> None:
    os.environ['EXEC_HEALTH_ACK_SIGNING_SECRET'] = 'test-secret'
    sig = sign_dual_control_commit(secret='test-secret', request_id='r1', ack_nonce='n1', prepared_by='alice', approved_by='bob', commit_by='bob', reason='validated', ticket='INC-1', trigger_ts_ms=10000, prepared_ts_ms=10500, approved_ts_ms=10800, commit_ts_ms=11000)
    res = evaluate_freeze_integrity(
        control_raw={'expected_ack_nonce': 'n1'},
        state_raw={},
        events=[
            ('4-0', {'kind': 'manual_ack_thaw_commit', 'request_id': 'r1', 'ack_nonce': 'n1', 'trigger_ts_ms': '10000', 'prepared_ts_ms': '10500', 'approved_ts_ms': '10800', 'ts_ms': '11000', 'operator': 'bob', 'prepared_by': 'alice', 'approved_by': 'bob', 'reason': 'validated', 'ticket': 'INC-1', 'commit_sig': sig}),
            ('2-0', {'kind': 'autoguard_freeze_latch', 'ack_nonce': 'n1', 'trigger_ts_ms': '10000'}),
        ],
    )
    assert res.valid_ack_event_present is True
    assert 'none' in res.violation_kinds
