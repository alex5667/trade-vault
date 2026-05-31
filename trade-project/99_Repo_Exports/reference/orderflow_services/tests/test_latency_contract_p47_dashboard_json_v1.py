from pathlib import Path
import json


def test_p47_dashboard_has_silence_panels() -> None:
    obj = json.loads(Path(__file__).resolve().parents[2].joinpath('orderflow_services/grafana/latency_contract_p47_v1.json').read_text(encoding='utf-8'))
    titles = [p.get('title', '') for p in obj.get('panels', [])]
    assert 'Unsilenced gate active total' in titles
    assert 'Notifier silenced' in titles
