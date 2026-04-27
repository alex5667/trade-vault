"""P4.7: Verify that the Grafana risk drift dashboard exists and contains expected panels."""
import json
from pathlib import Path


def test_p47_grafana_dashboard_exists_and_mentions_mismatch():
    """Risk drift dashboard must exist and contain Mismatch Rate and quarantine annotation."""
    p = (
        Path(__file__).resolve().parents[3]
        / 'monitoring'
        / 'grafana'
        / 'dashboards'
        / 'trade_execution_p47_risk_drift.json'
    )
    assert p.exists(), f'Dashboard not found: {p}'
    doc = json.loads(p.read_text(encoding='utf-8'))
    assert doc['title'].startswith('Trade Risk Drift')
    dumped = json.dumps(doc)
    assert 'Mismatch Rate' in dumped
    assert 'repeated_risk_consistency_mismatch' in dumped
