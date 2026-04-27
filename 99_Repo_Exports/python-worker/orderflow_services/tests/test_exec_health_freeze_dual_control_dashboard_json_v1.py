from __future__ import annotations

import json
from pathlib import Path


def test_exec_health_freeze_dual_control_dashboard_json_title_and_queries() -> None:
    obj = json.loads(Path('orderflow_services/grafana/exec_health_freeze_dual_control_v1.json').read_text())
    assert obj['title'] == 'ExecHealth Freeze Dual Control (v1)'
    q = json.dumps(obj)
    assert 'exec_health_freeze_dual_control_violation' in q
    assert 'exec_health_freeze_dual_control_valid_commit_event_present' in q
