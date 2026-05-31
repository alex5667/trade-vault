"""P4.14 tests: verify alerts YAML and dashboard JSON contain required metrics."""
from __future__ import annotations

import json
import os

import yaml
import pytest

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_alerts() -> dict:
    path = os.path.join(_REPO, 'prometheus_alerts_latency_contract_p414_v1.yml')
    with open(path) as f:
        return yaml.safe_load(f)


def _load_dashboard() -> dict:
    path = os.path.join(_REPO, 'grafana', 'latency_contract_p414_v1.json')
    with open(path) as f:
        return json.load(f)


class TestAlertsYaml:
    def test_file_loads(self):
        data = _load_alerts()
        assert 'groups' in data

    def test_route_binding_mismatch_alert_present(self):
        data = _load_alerts()
        alert_names = {rule['alert'] for group in data['groups'] for rule in group.get('rules', [])}
        assert 'LatencyContractDeployLintApprovalRouteBindingMismatch' in alert_names

    def test_notifier_route_page_drift_alert_present(self):
        data = _load_alerts()
        alert_names = {rule['alert'] for group in data['groups'] for rule in group.get('rules', [])}
        assert 'LatencyContractDeployLintApprovalNotifierRoutePageDrift' in alert_names

    def test_route_metric_in_alerts(self):
        data = _load_alerts()
        exprs = ' '.join(rule['expr'] for group in data['groups'] for rule in group.get('rules', []))
        assert 'latency_contract_deploy_lint_summary_dual_control_route_binding_mismatch_total' in exprs

    def test_notifier_route_class_match_metric_in_alerts(self):
        data = _load_alerts()
        exprs = ' '.join(rule['expr'] for group in data['groups'] for rule in group.get('rules', []))
        assert 'latency_contract_deploy_lint_silence_approval_notifier_route_class_match' in exprs


class TestDashboardJson:
    def test_file_loads(self):
        data = _load_dashboard()
        assert 'panels' in data

    def test_route_binding_mismatch_panel_present(self):
        data = _load_dashboard()
        exprs = json.dumps(data)
        assert 'latency_contract_deploy_lint_summary_dual_control_route_binding_mismatch_total' in exprs

    def test_notifier_route_class_match_panel_present(self):
        data = _load_dashboard()
        exprs = json.dumps(data)
        assert 'latency_contract_deploy_lint_silence_approval_notifier_route_class_match' in exprs

    def test_warning_policy_match_panel_present(self):
        data = _load_dashboard()
        exprs = json.dumps(data)
        assert 'latency_contract_deploy_lint_silence_approval_warning_policy_match' in exprs
