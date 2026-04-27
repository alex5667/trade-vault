from __future__ import annotations

import json
from pathlib import Path


def test_exec_health_freeze_hook_dashboard_json_title_and_queries() -> None:
    """Grafana dashboard JSON contains required title and metric queries."""
    obj = json.loads(Path("orderflow_services/grafana/exec_health_freeze_hook_v1.json").read_text())
    assert obj["title"] == "ExecHealth Freeze Hook (v1)"
    q = json.dumps(obj)
    assert "exec_health_freeze_hook_block_total" in q
    assert "exec_health_freeze_hook_active" in q
