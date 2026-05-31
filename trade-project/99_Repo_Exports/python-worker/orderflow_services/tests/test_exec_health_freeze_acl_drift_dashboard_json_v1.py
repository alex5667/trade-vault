from __future__ import annotations

import json
from pathlib import Path


def test_drift_dashboard_json_title_and_queries() -> None:
    p = Path("orderflow_services/grafana/exec_health_freeze_acl_drift_v1.json")
    obj = json.loads(p.read_text())
    assert obj["title"] == "ExecHealth Freeze ACL Drift (v1)"

    joined = json.dumps(obj)
    assert "exec_health_freeze_acl_contract_match" in joined
    assert "exec_health_freeze_acl_aclfile_configured" in joined
    assert "exec_health_freeze_acl_default_user_connections" in joined
    assert "exec_health_freeze_acl_unknown_user_connections" in joined
    assert "exec_health_freeze_acl_drift_state_age_seconds" in joined
