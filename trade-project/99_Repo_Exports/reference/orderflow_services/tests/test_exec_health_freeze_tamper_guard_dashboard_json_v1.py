from __future__ import annotations

import json
from pathlib import Path


def test_tamper_guard_dashboard_json_loads() -> None:
    path = Path('orderflow_services/grafana/exec_health_freeze_tamper_guard_v1.json')
    obj = json.loads(path.read_text())
    assert obj['title'] == 'ExecHealth Freeze Tamper Guard (v1)'
    titles = [p.get('title') for p in obj.get('panels', [])]
    assert 'Tamper Active' in titles
    assert 'Automatic Re-freeze Total' in titles
