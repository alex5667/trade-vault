from pathlib import Path

import yaml


def test_alerts_yaml_valid():
    data = yaml.safe_load(Path('orderflow_services/prometheus_alerts_exec_health_freeze_reconnect_nightly_v1.yml').read_text())
    assert data['groups'][0]['name'] == 'exec_health_freeze_reconnect_nightly_v1'
