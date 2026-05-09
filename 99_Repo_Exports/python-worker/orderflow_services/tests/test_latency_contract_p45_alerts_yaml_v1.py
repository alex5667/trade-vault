import os

import yaml


def test_p45_alerts_yaml_valid() -> None:
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'prometheus_alerts_latency_contract_p45_v1.yml'))
    with open(path, encoding='utf-8') as f:
        data = yaml.safe_load(f)
    assert 'groups' in data and data['groups']
