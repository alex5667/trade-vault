from __future__ import annotations

from pathlib import Path

import yaml


def test_exec_health_freeze_dual_control_alerts_yaml_has_expected_alerts() -> None:
    obj = yaml.safe_load(Path('orderflow_services/prometheus_alerts_exec_health_freeze_dual_control_v1.yml').read_text())
    alerts = [r.get('alert') for g in obj.get('groups', []) for r in g.get('rules', [])]
    assert 'OF_ExecHealth_FreezeDualControl_SameOperator_Crit' in alerts
    assert 'OF_ExecHealth_FreezeDualControl_InvalidCommit_Crit' in alerts
