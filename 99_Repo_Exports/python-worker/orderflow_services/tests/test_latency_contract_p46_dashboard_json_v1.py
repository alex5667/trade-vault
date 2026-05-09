import json
from pathlib import Path


def test_p46_dashboard_has_notifier_panels() -> None:
    obj = json.loads(Path(__file__).resolve().parents[2].joinpath('orderflow_services/grafana/latency_contract_p46_v1.json').read_text(encoding='utf-8'))
    titles = [p.get('title', '') for p in obj.get('panels', [])]
    assert 'Deploy-lint notifier active' in titles
