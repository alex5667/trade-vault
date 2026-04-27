"""P4.10 alerts YAML structural tests."""
from __future__ import annotations

import os
import yaml

_DIR = os.path.join(os.path.dirname(__file__), '..')
_FILE = os.path.join(_DIR, 'prometheus_alerts_latency_contract_p410_v1.yml')


def test_p410_alerts_yaml_loads():
    with open(_FILE) as f:
        data = yaml.safe_load(f)
    assert isinstance(data, dict)
    assert 'groups' in data


def test_p410_alerts_yaml_has_dual_control_rules():
    with open(_FILE) as f:
        data = yaml.safe_load(f)
    alert_names = [r['alert'] for g in data['groups'] for r in g['rules']]
    assert any('DualControl' in name for name in alert_names), f"No DualControl alert in {alert_names}"


def test_p410_alerts_yaml_valid_exprs():
    with open(_FILE) as f:
        data = yaml.safe_load(f)
    for g in data['groups']:
        for r in g['rules']:
            assert 'expr' in r and r['expr']
            assert 'for' in r
            assert 'labels' in r
            assert 'annotations' in r
