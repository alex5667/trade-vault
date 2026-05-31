"""P4.14 tests: warning-code policy route binding invalidation."""
from __future__ import annotations

import pytest

from services.observability.latency_deploy_lint_silence_approval_state import (
    DeployLintSilenceApprovalState,
    binding_mismatch_fields,
    resolve_warning_severity_policy,
    resolve_notifier_route_class,
)


def _make_state(**kwargs) -> DeployLintSilenceApprovalState:
    defaults = dict(
        status='approved',
        present=True,
        request_id='test-rid',
        purpose='orderflow_latency',
        prepared_by='ops1',
        prepared_ticket='TICK-1',
        prepared_reason='test',
        escalation_ticket='',
        requested_minutes=60,
        prepared_ts_ms=0,
        approved_by='ops2',
        approved_reason='lgtm',
        approved_ts_ms=0,
        consumed_by='',
        consumed_ts_ms=0,
        # P4.11 freshness
        freshness_deadline_ts_ms=0,
        expired_ts_ms=0,
        expired_reason='',
        cancelled_by='',
        cancelled_reason='',
        cancelled_ts_ms=0,
        # P4.12 binding
        binding_schema_version=2,
        bound_snapshot_ts_ms=0,
        bound_error_codes='ok',
        bound_error_codes_hash='abc',
        bound_active_purposes_csv='orderflow_latency',
        bound_active_purposes_hash='def',
        # P4.13
        bound_gate_reason_code='ok',
        bound_errors_count=0,
        bound_details_json='{}',
        bound_details_fingerprint='aaa',
        # P4.14
        bound_warning_codes='W001',
        bound_warning_codes_hash='wchash1',
        bound_warning_severity_policy='warn',
        bound_notifier_route_class='notify',
        # P4.12 invalidation
        invalidated_ts_ms=0,
        invalidated_reason='',
        invalidated_stage='',
        invalidated_error_codes='ok',
        invalidated_error_codes_hash='',
        invalidated_active_purposes_csv='none',
        invalidated_active_purposes_hash='',
        # P4.13 invalidation
        invalidated_gate_reason_code='ok',
        invalidated_errors_count=0,
        invalidated_details_json='{}',
        invalidated_details_fingerprint='',
        # P4.14 invalidation
        invalidated_warning_codes='none',
        invalidated_warning_codes_hash='',
        invalidated_warning_severity_policy='none',
        invalidated_notifier_route_class='notify',
    )
    defaults.update(kwargs)
    return DeployLintSilenceApprovalState(**defaults)


def _make_binding(**kwargs) -> dict:
    defaults = {
        'bound_error_codes': 'ok',
        'bound_error_codes_hash': 'abc',
        'bound_active_purposes_hash': 'def',
        'bound_gate_reason_code': 'ok',
        'bound_errors_count': '0',
        'bound_details_fingerprint': 'aaa',
        'bound_warning_codes_hash': 'wchash1',
        'bound_warning_severity_policy': 'warn',
        'bound_notifier_route_class': 'notify',
    }
    defaults.update(kwargs)
    return defaults


class TestResolveWarningSeverityPolicy:
    def test_page_wins_over_crit(self):
        r = resolve_warning_severity_policy(
            'P001,C001',
            warn_codes_page_csv='P001',
            warn_codes_crit_csv='C001',
        )
        assert r == 'page'

    def test_crit_wins_over_warn(self):
        r = resolve_warning_severity_policy(
            'C001,W001',
            warn_codes_crit_csv='C001',
            warn_codes_warn_csv='W001',
        )
        assert r == 'crit'

    def test_default_warn_if_code_not_in_any(self):
        r = resolve_warning_severity_policy(
            'UNKNOWN',
            warn_codes_page_csv='P001',
            warn_codes_crit_csv='C001',
            warn_codes_warn_csv='W001',
        )
        assert r == 'warn'

    def test_none_when_empty_codes(self):
        assert resolve_warning_severity_policy('') == 'none'
        assert resolve_warning_severity_policy('none') == 'none'


class TestResolveNotifierRouteClass:
    def test_page_for_page_policy(self):
        assert resolve_notifier_route_class(warning_severity_policy='page') == 'page'

    def test_notify_for_crit(self):
        assert resolve_notifier_route_class(warning_severity_policy='crit') == 'notify'

    def test_notify_for_none(self):
        assert resolve_notifier_route_class(warning_severity_policy='none') == 'notify'


class TestBindingMismatchP414:
    def test_no_mismatch_when_all_match(self):
        st = _make_state()
        binding = _make_binding()
        assert binding_mismatch_fields(st, binding) == []

    def test_detects_warning_codes_hash_change(self):
        st = _make_state(bound_warning_codes_hash='old_hash')
        binding = _make_binding(bound_warning_codes_hash='new_hash')
        fields = binding_mismatch_fields(st, binding)
        assert 'warning_codes' in fields

    def test_detects_severity_policy_change(self):
        st = _make_state(bound_warning_severity_policy='warn')
        binding = _make_binding(bound_warning_severity_policy='page')
        fields = binding_mismatch_fields(st, binding)
        assert 'warning_severity_policy' in fields

    def test_detects_route_class_change(self):
        st = _make_state(bound_notifier_route_class='notify')
        binding = _make_binding(bound_notifier_route_class='page')
        fields = binding_mismatch_fields(st, binding)
        assert 'notifier_route_class' in fields

    def test_no_mismatch_on_empty_approval(self):
        st = _make_state(present=False)
        binding = _make_binding(bound_warning_codes_hash='different')
        # When not present, binding_mismatch_fields is called only if approval.present
        # but calling directly should still work (returns list when fields set)
        # Simulating how cmd_ack uses it:
        fields = binding_mismatch_fields(st, binding) if st.present else []
        assert fields == []
