import json
from pathlib import Path


def test_dashboard_json_valid():
    data = json.loads(Path('orderflow_services/grafana/exec_health_freeze_reconnect_nightly_v1.json').read_text())
    assert data['title'] == 'ExecHealth Reconnect Nightly Smoke (v1)'
