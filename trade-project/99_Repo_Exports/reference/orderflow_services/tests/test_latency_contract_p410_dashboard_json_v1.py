"""P4.10 Grafana dashboard JSON structural tests."""
from __future__ import annotations

import json
import os

_DIR = os.path.join(os.path.dirname(__file__), '..')
_FILE = os.path.join(_DIR, 'grafana', 'latency_contract_p410_v1.json')


def test_p410_dashboard_loads():
    with open(_FILE) as f:
        data = json.load(f)
    assert isinstance(data, dict)
    assert 'panels' in data


def test_p410_dashboard_has_dual_control_panels():
    with open(_FILE) as f:
        data = json.load(f)
    titles = [p['title'] for p in data['panels']]
    assert any('dual' in t.lower() or 'approval' in t.lower() for t in titles), f"No dual-control panel in {titles}"


def test_p410_dashboard_panels_have_targets():
    with open(_FILE) as f:
        data = json.load(f)
    for p in data['panels']:
        assert 'targets' in p and p['targets'], f"Panel '{p.get('title')}' has no targets"
