from __future__ import annotations

"""Unit tests for the P7 Grafana dashboard JSON.

Validates that the dashboard file is valid JSON and has the required structure.
"""

import json
from pathlib import Path


def test_grafana_p7_dashboard_is_valid_json_and_has_panels():
    """Dashboard must be valid JSON with >=4 panels and correct title/uid."""
    path = (
        Path(__file__).resolve().parent.parent.parent.parent
        / 'monitoring' / 'grafana' / 'dashboards' / 'trade_execution_p7_panels.json'
    )
    doc = json.loads(path.read_text(encoding='utf-8'))
    assert doc['title'] == 'Trade Execution P7 Panels'
    assert doc['uid'] == 'trade-exec-p7'
    assert len(doc['panels']) >= 4


def test_grafana_p7_dashboard_panels_have_datasource():
    """Every panel must declare a Prometheus datasource."""
    path = (
        Path(__file__).resolve().parent.parent.parent.parent
        / 'monitoring' / 'grafana' / 'dashboards' / 'trade_execution_p7_panels.json'
    )
    doc = json.loads(path.read_text(encoding='utf-8'))
    for panel in doc['panels']:
        assert panel.get('datasource', {}).get('type') == 'prometheus', (
            f"Panel {panel.get('id')} is missing prometheus datasource"
        )


def test_grafana_p7_dashboard_has_required_tags():
    """Dashboard must carry the 'trade', 'execution', 'p7' tags."""
    path = (
        Path(__file__).resolve().parent.parent.parent.parent
        / 'monitoring' / 'grafana' / 'dashboards' / 'trade_execution_p7_panels.json'
    )
    doc = json.loads(path.read_text(encoding='utf-8'))
    tags = set(doc.get('tags', []))
    assert {'trade', 'execution', 'p7'}.issubset(tags)
