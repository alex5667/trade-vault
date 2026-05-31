"""Tests for P4.2 Prometheus alerts YAML."""
import os
import yaml


def test_p42_alerts_yaml_valid():
    """Alerts YAML must be valid and contain the expected P4.2 alert names."""
    path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', 'prometheus_alerts_latency_contract_p42_v1.yml')
    )
    with open(path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    assert 'groups' in data and data['groups']
    alert_names = [rule['alert'] for g in data['groups'] for rule in g.get('rules', [])]
    assert 'OF_LatencyContract_RolloutGate_Active_Crit' in alert_names
    assert 'OF_LatencyContract_ExternalCoverageMissing_Crit' in alert_names
    assert 'OF_LatencyContract_BudgetBreachSustained_Crit' in alert_names
