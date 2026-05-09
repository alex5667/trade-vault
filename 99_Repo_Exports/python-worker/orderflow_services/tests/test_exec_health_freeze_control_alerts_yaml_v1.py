from __future__ import annotations

from pathlib import Path

import yaml


def test_exec_health_freeze_control_alerts_yaml_has_expected_alerts() -> None:
    """YAML smoke: all three expected P7 alerts must be present."""
    obj = yaml.safe_load(
        Path('orderflow_services/prometheus_alerts_exec_health_freeze_control_v1.yml').read_text()
    )
    alerts = [r.get('alert') for g in obj.get('groups', []) for r in g.get('rules', [])]
    assert 'OF_ExecHealth_FreezeControl_ExporterStale_Warn' in alerts
    assert 'OF_ExecHealth_FreezeControl_AckPending_Warn' in alerts
    assert 'OF_ExecHealth_FreezeControl_ManualOverrideActive_Warn' in alerts


def test_exec_health_freeze_control_alerts_yaml_is_valid_structure() -> None:
    """YAML smoke: groups/rules structure must be valid."""
    obj = yaml.safe_load(
        Path('orderflow_services/prometheus_alerts_exec_health_freeze_control_v1.yml').read_text()
    )
    for group in obj.get('groups', []):
        assert 'name' in group
        for rule in group.get('rules', []):
            assert 'alert' in rule
            assert 'expr' in rule
            assert 'labels' in rule
            assert 'annotations' in rule
