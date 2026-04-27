from pathlib import Path
import yaml


def test_projection_alert_rules_yaml_loads():
    path = Path(__file__).parents[2] / 'orderflow_services' / 'prometheus_alerts_execution_projection_p121.yml'
    data = yaml.safe_load(path.read_text())
    assert isinstance(data, dict)
    rules = data['groups'][0]['rules']
    alerts = {rule['alert'] for rule in rules}
    assert 'TradeExecutionProjectionLagHigh' in alerts
    assert 'TradeExecutionProjectionCursorStalled' in alerts


def test_compose_contains_projection_worker_and_inline_projection_disabled():
    # The root docker-compose-crypto-orderflow.yml should contain projection worker
    path = Path(__file__).parents[3] / 'docker-compose-crypto-orderflow.yml'
    text = path.read_text()
    assert 'execution-state-projection-worker:' in text
    assert 'EXEC_INLINE_STATE_PROJECTION=0' in text
    assert 'EXEC_PROJECTION_CURSOR_KEY=orders:exec:projection:cursor' in text
