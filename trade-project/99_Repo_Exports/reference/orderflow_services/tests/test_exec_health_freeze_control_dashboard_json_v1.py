from __future__ import annotations

import json
from pathlib import Path


def test_exec_health_freeze_control_dashboard_json_title_and_queries() -> None:
    """Dashboard JSON smoke: title and key metric queries must be present."""
    obj = json.loads(Path('orderflow_services/grafana/exec_health_freeze_control_v1.json').read_text())
    assert obj['title'] == 'ExecHealth Freeze Control (v1)'
    q = json.dumps(obj)
    assert 'exec_health_freeze_control_effective_active' in q
    assert 'exec_health_freeze_control_manual_ack_required' in q
    assert 'exec_health_freeze_control_manual_override_active' in q


def test_exec_health_freeze_control_dashboard_json_panel_count() -> None:
    """Dashboard JSON smoke: must have 6 panels."""
    obj = json.loads(Path('orderflow_services/grafana/exec_health_freeze_control_v1.json').read_text())
    assert len(obj.get('panels', [])) == 6
