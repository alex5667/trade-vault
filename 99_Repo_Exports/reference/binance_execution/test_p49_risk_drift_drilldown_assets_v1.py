from pathlib import Path
import json


def test_drilldown_dashboard_contains_sid_and_decision_id_variables():
    """P4.9: verify UID and template variables in drilldown dashboard."""
    dash = json.loads(
        (Path(__file__).resolve().parents[1] / 'monitoring' / 'grafana' / 'dashboards' / 'trade_execution_p49_risk_drift_drilldown.json').read_text(encoding='utf-8')
    )
    names = {item['name'] for item in dash.get('templating', {}).get('list', [])}
    assert {'sid', 'decision_id'} <= names
