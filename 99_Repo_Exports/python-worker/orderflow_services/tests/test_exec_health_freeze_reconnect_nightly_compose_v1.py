from pathlib import Path

import yaml


def test_compose_yaml_valid():
    data = yaml.safe_load(Path('orderflow_services/deploy/docker-compose.exec-health-freeze-reconnect-nightly-v1.yml').read_text())
    svc = data['services']['exec-health-freeze-reconnect-nightly']
    assert svc['working_dir'] == '/repo'
    assert 'REDIS_URL' in svc['environment']
    assert 'python -m orderflow_services.exec_health_freeze_reconnect_nightly_smoke_v1' in ' '.join(svc['command'])
