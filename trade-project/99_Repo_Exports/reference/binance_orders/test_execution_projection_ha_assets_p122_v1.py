"""Asset tests: P1.2.2 HA projection worker — YAML and docker-compose checks."""

from __future__ import annotations

from pathlib import Path


def test_ha_alert_rules_yaml_loads_and_contains_p122_alerts():
    """P1.2.2 Prometheus rules YAML must load and contain both HA alerts."""
    import yaml

    path = (
        Path(__file__).parents[2]
        / 'orderflow_services'
        / 'prometheus_alerts_execution_projection_ha_p122.yml'
    )
    assert path.exists(), f'Missing alert rules file: {path}'
    data = yaml.safe_load(path.read_text())
    assert isinstance(data, dict)
    assert 'groups' in data
    rules = data['groups'][0]['rules']
    alert_names = {rule['alert'] for rule in rules}
    assert 'TradeExecutionProjectionNoLeader' in alert_names
    assert 'TradeExecutionProjectionClusterNotReady' in alert_names


def test_compose_contains_p122_ha_env_and_health_service():
    """docker-compose must have lease env vars and the health sidecar service."""
    root_compose = Path(__file__).parents[3] / 'docker-compose-crypto-orderflow.yml'
    assert root_compose.exists(), f'Missing compose file: {root_compose}'
    text = root_compose.read_text()

    # P1.2.1 baseline must still be present
    assert 'execution-state-projection-worker:' in text
    assert 'EXEC_INLINE_STATE_PROJECTION=0' in text

    # P1.2.2 lease knobs
    assert 'EXEC_PROJECTION_LEASE_ENABLE=1' in text
    assert 'EXEC_PROJECTION_LEASE_KEY=orders:exec:projection:leader' in text
    assert 'EXEC_PROJECTION_FENCE_KEY=orders:exec:projection:fence' in text

    # P1.2.2 health sidecar
    assert 'execution-state-projection-health' in text
