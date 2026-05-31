from __future__ import annotations

"""Tests that the P4.1 Grafana dashboard JSON is valid."""

import json
import os

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


def test_dashboard_json_valid():
    path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', 'grafana', 'latency_contract_p41_v1.json')
    )
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    assert data['title']
    assert data['panels']
    panel_titles = {p['title'] for p in data['panels']}
    assert 'SLO Gate OK' in panel_titles
    assert 'Required Stage Presence' in panel_titles
