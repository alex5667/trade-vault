from __future__ import annotations

from pathlib import Path
import json


def test_exec_health_freeze_acl_dashboard_json_title_and_queries() -> None:
    p = Path('orderflow_services/grafana/exec_health_freeze_acl_v1.json')
    obj = json.loads(p.read_text())
    assert obj['title'] == 'ExecHealth Freeze ACL / Sealed State (v1)'
    joined = json.dumps(obj)
    assert 'exec_health_freeze_acl_violation_total' in joined
    assert 'exec_health_freeze_acl_audit_state_age_seconds' in joined
