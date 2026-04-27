"""Tests for P4.2 Grafana dashboard JSON."""
import json
import os


def test_p42_dashboard_json_valid():
    """Dashboard JSON must be valid and contain panels and title."""
    path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', 'grafana', 'latency_contract_p42_v1.json')
    )
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    assert data['title']
    assert data['panels']
    # Must include at least the rollout gate active panel.
    titles = [p.get('title', '') for p in data['panels']]
    assert any('gate' in t.lower() for t in titles)
