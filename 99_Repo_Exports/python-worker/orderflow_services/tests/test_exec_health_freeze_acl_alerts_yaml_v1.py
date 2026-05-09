from __future__ import annotations

from pathlib import Path

import yaml


def test_exec_health_freeze_acl_alerts_yaml_contains_expected_rules() -> None:
    p = Path('orderflow_services/prometheus_alerts_exec_health_freeze_acl_v1.yml')
    obj = yaml.safe_load(p.read_text())
    rules = [r['alert'] for g in obj['groups'] for r in g['rules']]
    assert 'OF_ExecHealth_FreezeACLViolation_Crit' in rules
    assert 'OF_ExecHealth_FreezeACLAuditExporter_Stale_Warn' in rules
