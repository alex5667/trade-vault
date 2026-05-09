from __future__ import annotations

"""P8 tests: prometheus alerts YAML structure."""

import yaml


def test_alerts_yaml_loads_and_has_required_alerts() -> None:
    """Alerts YAML must be valid and contain the three P8 rules."""
    with open(
        "orderflow_services/prometheus_alerts_exec_health_freeze_integrity_v1.yml"
    ) as fh:
        d = yaml.safe_load(fh)
    assert "groups" in d
    rules = {r["alert"]: r for g in d["groups"] for r in g.get("rules", [])}
    assert "OF_ExecHealth_FreezeIntegrity_TamperDetected_Crit" in rules
    assert "OF_ExecHealth_FreezeIntegrity_ExporterStale_Warn" in rules
    assert "OF_ExecHealth_FreezeIntegrity_PendingAckStuck_Warn" in rules
    # critical rule must reference the violation metric
    expr = rules["OF_ExecHealth_FreezeIntegrity_TamperDetected_Crit"]["expr"]
    assert "exec_health_freeze_integrity_violation" in expr
