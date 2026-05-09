import os

import yaml


def test_prometheus_rules_bundle_valid():
    """Validates the DQ gate policy prometheus rules bundle yaml."""
    rules_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        'prometheus_alerts_dq_gate_policy_v1.yml'
    )
    assert os.path.exists(rules_path), f"Rules file not found at {rules_path}"

    with open(rules_path) as f:
        data = yaml.safe_load(f)

    assert 'groups' in data, "No groups found in yaml"

    # Track the alerts defined in the file
    alerts_found = []
    for group in data['groups']:
        for rule in group.get('rules', []):
            if 'alert' in rule:
                alerts_found.append(rule['alert'])
                assert 'expr' in rule, f"Rule {rule['alert']} missing expr"
                assert 'for' in rule, f"Rule {rule['alert']} missing for"
                assert 'labels' in rule, f"Rule {rule['alert']} missing labels"
                assert 'severity' in rule['labels'], f"Rule {rule['alert']} missing severity"

    expected_alerts = [
        "OF_DQ_GateHardState_Crit",
        "OF_DQ_GateVetoRate_High_Warn",
        "OF_DQ_TickGapP95High_Warn",
        "OF_DQ_TickMissingSeqEmaHard_Crit"
    ]
    for exp in expected_alerts:
        assert exp in alerts_found, f"Expected alert {exp} not found in rules bundle."
