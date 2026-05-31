"""P4.9 test: Grafana dashboard JSON has required policy panels."""
from pathlib import Path
import json


def test_p49_dashboard_has_policy_panels() -> None:
    obj = json.loads(Path(__file__).resolve().parents[2].joinpath('orderflow_services/grafana/latency_contract_p49_v1.json').read_text(encoding='utf-8'))
    titles = [p.get('title', '') for p in obj.get('panels', [])]
    assert 'Policy blocked gate active total' in titles
    assert 'Policy override gate active total' in titles
    assert 'Policy budget minutes used' in titles
