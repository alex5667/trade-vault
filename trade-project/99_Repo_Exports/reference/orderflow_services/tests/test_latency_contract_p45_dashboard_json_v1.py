import json
import os


def test_p45_dashboard_json_valid() -> None:
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'grafana', 'latency_contract_p45_v1.json'))
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    assert data['title']
    assert data['panels']
