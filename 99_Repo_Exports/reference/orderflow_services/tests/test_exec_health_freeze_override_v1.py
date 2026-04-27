from __future__ import annotations

import os

from services.orderflow.exec_health_freeze_control import parse_exec_health_freeze_control, verify_dual_control_commit_signature
from orderflow_services.exec_health_freeze_override_v1 import OverrideController


class FakeRedis:
    def __init__(self):
        self.hashes = {
            'cfg:orderflow:exec_health:freeze_control:v1': {
                'effective_freeze_active': '1',
                'manual_ack_required': '1',
                'expected_ack_nonce': 'n1',
                'thaw_request_nonce': 'n1',
                'last_trigger_ts_ms': '10000',
                'updated_ts_ms': '10000',
            },
            'metrics:exec_health:slo:autoguard:state': {},
        }
        self.stream = []
        self.strings = {}

    def hgetall(self, key: str):
        return dict(self.hashes.get(key, {}))

    def hset(self, key: str, mapping: dict):
        self.hashes.setdefault(key, {}).update(mapping)

    def expire(self, key: str, ttl: int):
        pass

    def get(self, key: str):
        return self.strings.get(key)

    def set(self, key: str, value):
        self.strings[key] = value

    def pexpire(self, key: str, ttl: int):
        pass

    def xadd(self, key: str, mapping, maxlen=0):
        self.stream.append((key, dict(mapping)))
        return f'{len(self.stream)}-0'


def _ctl():
    c = OverrideController.__new__(OverrideController)
    c.control_key = 'cfg:orderflow:exec_health:freeze_control:v1'
    c.state_key = 'metrics:exec_health:slo:autoguard:state'
    c.freeze_key = 'cfg:orderflow:exec_health:auto_freeze:v1'
    c.event_stream = 'ops:exec_health:freeze_events:v1'
    c.r = FakeRedis()
    return c


def test_prepare_approve_commit_thaw_persists_dual_control_fields() -> None:
    os.environ['EXEC_HEALTH_ACK_SIGNING_SECRET'] = 'test-secret'
    c = _ctl()
    prep = c.prepare_thaw(operator='alice', reason='validated rollback', ticket='INC-42', nonce='n1')
    rid = prep['request_id']
    appr = c.approve_thaw(operator='bob', request_id=rid)
    out = c.commit_thaw(operator='bob', request_id=rid)
    assert out['ok'] is True
    st = parse_exec_health_freeze_control(c.r.hashes[c.control_key])
    assert st.effective_freeze_active is False
    assert st.active_thaw_request_id == rid
    assert st.thaw_prepared_by == 'alice'
    assert st.thaw_approved_by == 'bob'
    assert verify_dual_control_commit_signature(c.r.hashes[c.control_key], secret='test-secret') is True
    assert c.r.stream[-1][1]['kind'] == 'manual_ack_thaw_commit'


def test_approve_requires_distinct_second_operator() -> None:
    c = _ctl()
    prep = c.prepare_thaw(operator='alice', reason='validated rollback', ticket='INC-42', nonce='n1')
    try:
        c.approve_thaw(operator='alice', request_id=prep['request_id'])
    except ValueError as e:
        assert 'different from preparer' in str(e)
    else:
        raise AssertionError('expected dual-control failure')


def test_commit_requires_approved_request() -> None:
    os.environ['EXEC_HEALTH_ACK_SIGNING_SECRET'] = 'test-secret'
    c = _ctl()
    prep = c.prepare_thaw(operator='alice', reason='validated rollback', ticket='INC-42', nonce='n1')
    try:
        c.commit_thaw(operator='bob', request_id=prep['request_id'])
    except ValueError as e:
        assert 'not approved' in str(e)
    else:
        raise AssertionError('expected approval gate failure')
