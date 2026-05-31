from __future__ import annotations

import json
from pathlib import Path


def test_grafana_p8_dashboard_has_alert_annotations():
    path = Path(__file__).resolve().parent.parent.parent / 'monitoring' / 'grafana' / 'dashboards' / 'trade_execution_p8_annotations.json'
    data = json.loads(path.read_text())
    names = [item.get('name') for item in data.get('annotations', {}).get('list', [])]
    assert 'Trade alert annotations' in names
    assert 'Quarantine / repair events' in names
