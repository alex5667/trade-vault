from __future__ import annotations

from pathlib import Path

import yaml


def test_drift_alerts_yaml_contains_expected_rules() -> None:
    p = Path("orderflow_services/prometheus_alerts_exec_health_freeze_acl_drift_v1.yml")
    obj = yaml.safe_load(p.read_text())
    rules = [r["alert"] for g in obj["groups"] for r in g["rules"]]

    assert "OF_ExecHealth_FreezeACLDrift_Crit" in rules
    assert "OF_ExecHealth_FreezeDefaultUserConnected_Crit" in rules
    assert "OF_ExecHealth_FreezeACLFileMissing_Warn" in rules
    assert "OF_ExecHealth_FreezeACLDriftExporter_Stale_Warn" in rules
