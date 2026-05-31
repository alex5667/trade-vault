from __future__ import annotations

from pathlib import Path

import yaml


def test_tamper_guard_alerts_yaml_loads() -> None:
    path = Path('orderflow_services/prometheus_alerts_exec_health_freeze_tamper_guard_v1.yml')
    obj = yaml.safe_load(path.read_text())
    names = [r['alert'] for g in obj.get('groups', []) for r in g.get('rules', []) if 'alert' in r]
    assert 'OF_ExecHealth_FreezeTamperDetected_Crit' in names
    assert 'OF_ExecHealth_FreezeTamperRefreezePerformed_Warn' in names
