from __future__ import annotations

from pathlib import Path

import yaml


def test_exec_health_freeze_hook_alerts_yaml_has_expected_alerts() -> None:
    """Prometheus alerts YAML contains both required alert names."""
    obj = yaml.safe_load(Path("orderflow_services/prometheus_alerts_exec_health_freeze_hook_v1.yml").read_text())
    alerts = [r.get("alert") for g in obj.get("groups", []) for r in g.get("rules", [])]
    assert "OF_ExecHealth_FreezeHook_Blocks_Warn" in alerts
    assert "OF_ExecHealth_FreezeHook_ReaderErrors_Warn" in alerts
