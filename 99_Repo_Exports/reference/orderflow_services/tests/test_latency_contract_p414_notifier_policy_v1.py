"""P4.14 tests: warning-code policy aware notifier route selection."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from orderflow_services.latency_contract_deploy_lint_notifier_v1 import (
    Cfg,
    _warning_policy_for_active,
    _route_class_for_event,
    _severity_for_event,
    _notify_stream_for_event,
)


def _make_cfg(**kwargs) -> Cfg:
    defaults = dict(
        redis_url='redis://localhost:6379/0',
        state_prefix='metrics:latency_contract:deploy_lint:last',
        notifier_state_key='metrics:latency_contract:deploy_lint:notifier:last',
        silence_prefix='cfg:orderflow:latency_contract:deploy_lint:silence',
        ops_stream='ops:latency_contract:events:v1',
        notify_stream='notify:telegram',
        notify_page_stream='notify:telegram:page',
        notify_enable=True,
        reminder_s=21600,
        state_ttl_s=172800,
        warn_codes_warn_csv='W001',
        warn_codes_crit_csv='C001',
        warn_codes_page_csv='P001',
    )
    defaults.update(kwargs)
    return Cfg(**defaults)


class TestWarningPolicyForActive:
    def test_empty_active_returns_none(self):
        cfg = _make_cfg()
        assert _warning_policy_for_active(cfg, {}, []) == 'none'

    def test_page_code_returns_page(self):
        cfg = _make_cfg()
        details = {'purpose_a': {'warning_codes': 'P001'}}
        assert _warning_policy_for_active(cfg, details, ['purpose_a']) == 'page'

    def test_crit_code_returns_crit(self):
        cfg = _make_cfg()
        details = {'purpose_a': {'warning_codes': 'C001'}}
        assert _warning_policy_for_active(cfg, details, ['purpose_a']) == 'crit'

    def test_warn_code_returns_warn(self):
        cfg = _make_cfg()
        details = {'purpose_a': {'warning_codes': 'W001'}}
        assert _warning_policy_for_active(cfg, details, ['purpose_a']) == 'warn'

    def test_page_dominates_crit(self):
        cfg = _make_cfg()
        details = {
            'purpose_a': {'warning_codes': 'C001'},
            'purpose_b': {'warning_codes': 'P001'},
        }
        assert _warning_policy_for_active(cfg, details, ['purpose_a', 'purpose_b']) == 'page'

    def test_unknown_codes_fall_back_to_warn(self):
        cfg = _make_cfg()
        details = {'purpose_a': {'warning_codes': 'ZZZZ'}}
        assert _warning_policy_for_active(cfg, details, ['purpose_a']) == 'warn'


class TestRouteClassForEvent:
    def test_page_policy_routes_to_page(self):
        assert _route_class_for_event('latency_deploy_lint_persistent_drift', 'page') == 'page'

    def test_crit_policy_routes_to_notify(self):
        assert _route_class_for_event('latency_deploy_lint_persistent_drift', 'crit') == 'notify'

    def test_ttl_expired_always_pages(self):
        assert _route_class_for_event('latency_deploy_lint_silence_ttl_expired_reactivated', 'warn') == 'page'

    def test_recovered_routes_to_notify(self):
        assert _route_class_for_event('latency_deploy_lint_recovered', 'none') == 'notify'


class TestSeverityForEvent:
    def test_recovered_is_info(self):
        assert _severity_for_event('latency_deploy_lint_recovered', 'page') == 'info'

    def test_ttl_expired_is_page(self):
        assert _severity_for_event('latency_deploy_lint_silence_ttl_expired_reactivated', 'none') == 'page'

    def test_page_policy_is_page(self):
        assert _severity_for_event('latency_deploy_lint_persistent_drift', 'page') == 'page'

    def test_default_drift_is_crit(self):
        assert _severity_for_event('latency_deploy_lint_persistent_drift', 'warn') == 'crit'


class TestNotifyStreamForEvent:
    def test_page_policy_returns_page_stream(self):
        cfg = _make_cfg()
        stream = _notify_stream_for_event(cfg, 'latency_deploy_lint_persistent_drift', 'page')
        assert stream == 'notify:telegram:page'

    def test_warn_policy_returns_notify_stream(self):
        cfg = _make_cfg()
        stream = _notify_stream_for_event(cfg, 'latency_deploy_lint_persistent_drift', 'warn')
        assert stream == 'notify:telegram'
