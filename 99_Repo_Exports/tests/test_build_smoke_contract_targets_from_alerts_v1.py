import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock

import ml_analysis.tools.build_smoke_contract_targets_from_alerts_v1 as mod

def test_normalize_runbook_path():
    assert mod._normalize_runbook_path("/web_uptime.md") == "/runbooks/web_uptime.md"
    assert mod._normalize_runbook_path("web_uptime.md") == "/runbooks/web_uptime.md"
    assert mod._normalize_runbook_path("/runbooks/web_uptime.md") == "/runbooks/web_uptime.md"
    assert mod._normalize_runbook_path("") == ""

def test_normalize_dashboard_path():
    assert mod._normalize_dashboard_path("/d/123/dash") == "/grafana/d/123/dash"
    assert mod._normalize_dashboard_path("d/123/dash") == "/grafana/d/123/dash"
    assert mod._normalize_dashboard_path("/grafana/d/123/dash") == "/grafana/d/123/dash"
    assert mod._normalize_dashboard_path("") == ""

def test_build_targets_from_alerts():
    # create a mock yaml
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        f.write("""
groups:
  - rules:
      - labels:
          severity: critical
        annotations:
          runbook_path: /runbooks/crit.md
          dashboard_path: /grafana/d/crit
      - labels:
          severity: warning
        annotations:
          runbook_path: /warn.md
          dashboard_path: /d/warn
      - labels:
          severity: info
        annotations:
          runbook_path: /info.md
          dashboard_path: /d/info
""")
        temp_name = f.name
    
    try:
        runbooks, dashboards = mod.build_targets_from_alerts(temp_name)
        assert sorted(runbooks) == ["/runbooks/crit.md", "/runbooks/warn.md"]
        assert sorted(dashboards) == ["/grafana/d/crit", "/grafana/d/warn"]
    finally:
        os.remove(temp_name)

@patch.dict("sys.modules", {"redis": MagicMock()})
@patch.dict(os.environ, {"REDIS_URL": "redis://fake:6379/1"}, clear=True)
def test_write_targets_to_redis():
    import sys
    mock_r = MagicMock()
    sys.modules["redis"].Redis.from_url.return_value = mock_r
    
    res = mod.write_targets_to_redis(["/runbooks/x.md"], ["/grafana/d/x"])
    assert res["ok"] is True
    assert res["runbooks"] == 1
    assert res["dashboards"] == 1
    
    mock_r.hset.assert_called_once()
    mock_r.expire.assert_called_once()
    
def test_write_targets_to_redis_no_url():
    with patch.dict(os.environ, clear=True):
        res = mod.write_targets_to_redis([], [])
        assert res["ok"] is False
        assert "not set" in res["error"]
