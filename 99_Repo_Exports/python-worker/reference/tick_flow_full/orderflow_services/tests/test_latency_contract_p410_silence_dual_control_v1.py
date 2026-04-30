"""P4.10 dual-control silence approval workflow tests."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from services.observability.latency_deploy_lint_silence_approval_state import (
    approve_override_approval
    consume_approval
    parse_approval_state
    prepare_override_approval
    validate_approval_for_ack
)
from services.observability.latency_deploy_lint_silence_state import (
    _base_mapping
    evaluate_ack_policy
    parse_silence_state
    record_dual_control_denial
    upsert_ack_silence
)
from orderflow_services.latency_contract_deploy_lint_silence_v1 import (
    Cfg
    cmd_ack
    cmd_approve_override
    cmd_prepare_override
)


def _fake_redis():
    """Minimal in-memory Redis stub that also handles set/get for latest-key pointers."""
    store: dict = {}

    class FakeR:
        def hgetall(self, k):
            return dict(store.get(k, {}))

        def hset(self, k, mapping=None, **kw):
            if k not in store or not isinstance(store.get(k), dict):
                store[k] = {}
            store[k].update(mapping or {})

        def expire(self, k, ttl):
            pass

        def set(self, k, v, ex=None):
            store[k] = v

        def get(self, k):
            v = store.get(k, '')
            return v if isinstance(v, str) else ''

        def xadd(self, stream, fields, maxlen=None, approximate=None):
            return b'1-1'

    return FakeR(), store


def _cfg(**overrides) -> Cfg:
    base = dict(
        redis_url='redis://localhost:6379/0'
        state_prefix='s'
        silence_prefix='sil'
        approval_prefix='appr'
        ops_stream='ops:test'
        silence_ttl_s=86400
        approval_ttl_s=604800
        default_minutes=360
        policy_window_s=168 * 3600
        policy_max_budget_minutes=1440
        policy_max_acks=3
        policy_denied_exit_code=27
        dual_control_minutes=480
    )
    base.update(overrides)
    return Cfg(**base)


def _force_override(r, cfg, purpose='rollout', now_ms=1_000_000_000):
    """Exhaust ack budget (3 acks) so next one requires escalation override."""
    for i in range(3):
        upsert_ack_silence(
            r
            prefix=cfg.silence_prefix
            purpose=purpose
            operator='op'
            ticket=f'T-{i}'
            reason='test'
            silence_minutes=500
            ttl_s=cfg.silence_ttl_s
            ops_stream=cfg.ops_stream
            gate_active=True
            now_ms=now_ms + i * 1000
            policy_window_s=cfg.policy_window_s
            policy_max_budget_minutes=cfg.policy_max_budget_minutes
            policy_max_acks=cfg.policy_max_acks
            escalation_ticket=f'ESC-{i}' if i > 0 else ''
        )


class TestApprovalStateMachine:
    def test_prepare_creates_prepared_state(self):
        r, store = _fake_redis()
        raw = prepare_override_approval(
            r, prefix='appr', purpose='rollout', operator='alice'
            ticket='T-1', escalation_ticket='ESC-1', reason='long maint'
            minutes=600, ttl_s=604800, ops_stream=None, now_ms=1_000_000_000
        )
        st = parse_approval_state(raw)
        assert st.status == 'prepared'
        assert st.prepared_by == 'alice'
        assert st.requested_minutes == 600
        assert st.purpose == 'rollout'

    def test_approve_transitions_to_approved(self):
        r, store = _fake_redis()
        raw = prepare_override_approval(
            r, prefix='appr', purpose='rollout', operator='alice'
            ticket='T-1', escalation_ticket='ESC-1', reason='r'
            minutes=600, ttl_s=604800, ops_stream=None, now_ms=1_000_000_000
        )
        rid = raw['request_id']
        raw2 = approve_override_approval(
            r, prefix='appr', request_id=rid, operator='bob'
            reason='approved', ttl_s=604800, ops_stream=None, now_ms=1_000_000_001
        )
        st = parse_approval_state(raw2)
        assert st.status == 'approved'
        assert st.approved_by == 'bob'

    def test_self_approve_rejected(self):
        r, store = _fake_redis()
        raw = prepare_override_approval(
            r, prefix='appr', purpose='rollout', operator='alice'
            ticket='T-1', escalation_ticket='ESC-1', reason='r'
            minutes=600, ttl_s=604800, ops_stream=None, now_ms=1_000_000_000
        )
        rid = raw['request_id']
        with pytest.raises(ValueError, match='different from requester'):
            approve_override_approval(
                r, prefix='appr', request_id=rid, operator='alice'
                reason='self', ttl_s=604800, ops_stream=None, now_ms=1_000_000_001
            )

    def test_consume_transitions_to_consumed(self):
        r, store = _fake_redis()
        raw = prepare_override_approval(
            r, prefix='appr', purpose='rollout', operator='alice'
            ticket='T-1', escalation_ticket='ESC-1', reason='r'
            minutes=600, ttl_s=604800, ops_stream=None, now_ms=1_000_000_000
        )
        rid = raw['request_id']
        approve_override_approval(
            r, prefix='appr', request_id=rid, operator='bob'
            reason='ok', ttl_s=604800, ops_stream=None, now_ms=1_000_000_001
        )
        raw3 = consume_approval(
            r, prefix='appr', request_id=rid, operator='alice'
            ttl_s=604800, ops_stream=None, now_ms=1_000_000_002
        )
        st = parse_approval_state(raw3)
        assert st.status == 'consumed'
        assert st.consumed_by == 'alice'


class TestValidateApprovalForAck:
    def _approved_raw(self, r, prefix='appr', operator='alice', purpose='rollout'
                      ticket='T-1', esc='ESC-1', minutes=600):
        raw = prepare_override_approval(
            r, prefix=prefix, purpose=purpose, operator=operator
            ticket=ticket, escalation_ticket=esc, reason='r'
            minutes=minutes, ttl_s=604800, ops_stream=None, now_ms=1_000_000_000
        )
        rid = raw['request_id']
        approve_override_approval(
            r, prefix=prefix, request_id=rid, operator='bob'
            reason='ok', ttl_s=604800, ops_stream=None, now_ms=1_000_000_001
        )
        return r.hgetall(f"{prefix}:req:{rid}")

    def test_valid_approval_accepted(self):
        r, _ = _fake_redis()
        raw = self._approved_raw(r)
        v = validate_approval_for_ack(raw, purpose='rollout', operator='alice', ticket='T-1', escalation_ticket='ESC-1', minutes=600)
        assert v.ok

    def test_missing_approval_rejected(self):
        v = validate_approval_for_ack({}, purpose='rollout', operator='alice', ticket='T-1', escalation_ticket='ESC-1', minutes=600)
        assert not v.ok
        assert 'missing' in v.reason

    def test_wrong_minutes_rejected(self):
        r, _ = _fake_redis()
        raw = self._approved_raw(r, minutes=600)
        v = validate_approval_for_ack(raw, purpose='rollout', operator='alice', ticket='T-1', escalation_ticket='ESC-1', minutes=700)
        assert not v.ok
        assert 'minutes_mismatch' in v.reason

    def test_wrong_operator_rejected(self):
        r, _ = _fake_redis()
        raw = self._approved_raw(r, operator='alice')
        v = validate_approval_for_ack(raw, purpose='rollout', operator='charlie', ticket='T-1', escalation_ticket='ESC-1', minutes=600)
        assert not v.ok
        assert 'requester_mismatch' in v.reason


class TestDualControlGateInCmdAck:
    def test_long_override_requires_approval_when_override_active(self):
        """Long window with active override and no approval → denied with dual_control_required=True."""
        r, store = _fake_redis()
        cfg = _cfg(dual_control_minutes=480)
        T0 = 1_000_000_000
        # First ack uses 1500 min over budget (1440 max) → requires escalation for the next attempt
        # But with escalation_ticket ESC-1, it goes through as an override
        upsert_ack_silence(
            r, prefix=cfg.silence_prefix, purpose='rollout', operator='op', ticket='T-1'
            reason='test', silence_minutes=1500, ttl_s=cfg.silence_ttl_s
            ops_stream=None, gate_active=True, now_ms=T0
            policy_window_s=cfg.policy_window_s
            policy_max_budget_minutes=cfg.policy_max_budget_minutes
            policy_max_acks=cfg.policy_max_acks
            escalation_ticket='ESC-1'
        )
        store['s:rollout'] = {'gate_active': '1'}
        # Same window: only 1 second later → budget still exhausted, override needed
        # Long ack (600 min >= 480 threshold) with valid new escalation ticket but NO approval → dual-control denied
        out = cmd_ack(
            r, cfg, purpose='rollout', operator='op', ticket='T-2'
            reason='long', minutes=600, escalation_ticket='ESC-2'
            approval_request_id='', now_ms=T0 + 1000
        )
        assert out['ok'] is False
        assert out['policy']['dual_control_required'] is True
        assert 'dual_control' in out['policy']['denied_reason']

    def test_full_prepare_approve_ack_workflow(self):
        """Happy path: prepare → approve → ack with valid request_id succeeds."""
        r, store = _fake_redis()
        cfg = _cfg(dual_control_minutes=480)
        T0 = 1_000_000_000
        # Force escalation required: budget 1500 > 1440 max, uses ESC-1 override
        upsert_ack_silence(
            r, prefix=cfg.silence_prefix, purpose='rollout', operator='op', ticket='T-1'
            reason='test', silence_minutes=1500, ttl_s=cfg.silence_ttl_s
            ops_stream=None, gate_active=True, now_ms=T0
            policy_window_s=cfg.policy_window_s
            policy_max_budget_minutes=cfg.policy_max_budget_minutes
            policy_max_acks=cfg.policy_max_acks
            escalation_ticket='ESC-1'
        )
        store['s:rollout'] = {'gate_active': '1'}

        # Step 1: prepare (within same window: T0+1000)
        prep = cmd_prepare_override(
            r, cfg, purpose='rollout', operator='op', ticket='T-2'
            escalation_ticket='ESC-2', reason='long maint', minutes=600
            now_ms=T0 + 1000
        )
        assert prep['ok']
        rid = prep['request_id']

        # Step 2: approve (different operator, T0+2000)
        appr = cmd_approve_override(r, cfg, request_id=rid, operator='op2', reason='ok', now_ms=T0 + 2000)
        assert appr['ok']

        # Step 3: ack with approved request_id (within same window T0+3000)
        out = cmd_ack(
            r, cfg, purpose='rollout', operator='op', ticket='T-2'
            reason='long maint', minutes=600, escalation_ticket='ESC-2'
            approval_request_id=rid, now_ms=T0 + 3000
        )
        assert out['ok'] is True, f"Expected ok but got denied_reason={out.get('policy', {}).get('denied_reason')}"
        assert out['policy']['dual_control_required'] is True


class TestBaseMapping:
    def test_base_mapping_carries_dual_control_fields(self):
        prev = {
            'dual_control_required': '1'
            'dual_control_request_id': 'abc'
            'dual_control_denied_total': '3'
        }
        m = _base_mapping(prev, purpose='rollout')
        assert m['dual_control_required'] == '1'
        assert m['dual_control_request_id'] == 'abc'
        assert m['dual_control_denied_total'] == '3'
        assert m['schema_version'] == '3'


class TestRecordDualControlDenial:
    def test_denial_increments_counter(self):
        r, store = _fake_redis()
        record_dual_control_denial(
            r, prefix='sil', purpose='rollout', operator='op', ticket='T-1'
            escalation_ticket='ESC-1', reason='test', silence_minutes=600
            deny_reason='dual_control_approval_missing', ttl_s=86400, ops_stream=None
            now_ms=1_000_000_000
        )
        raw = store.get('sil:rollout', {})
        assert raw.get('dual_control_denied_total') == '1'
        assert raw.get('dual_control_last_deny_reason') == 'dual_control_approval_missing'
        assert raw.get('last_action') == 'ack_denied_dual_control'
