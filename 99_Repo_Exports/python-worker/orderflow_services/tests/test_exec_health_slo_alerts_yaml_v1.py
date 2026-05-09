from __future__ import annotations

"""Tests for prometheus_alerts_exec_health_slo_v1.yml (P4)."""
import os
import unittest

import yaml


class TestExecHealthSloAlertsYaml(unittest.TestCase):
    def _get_path(self) -> str:
        candidates = [
            os.path.join(os.path.dirname(__file__), "..", "prometheus_alerts_exec_health_slo_v1.yml"),
            "orderflow_services/prometheus_alerts_exec_health_slo_v1.yml",
            "python-worker/orderflow_services/prometheus_alerts_exec_health_slo_v1.yml",
        ]
        for p in candidates:
            if os.path.isfile(p):
                return os.path.abspath(p)
        raise unittest.SkipTest("prometheus_alerts_exec_health_slo_v1.yml not found")

    def test_yaml_parses_and_has_required_alerts(self):
        path = self._get_path()
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)

        self.assertIn("groups", data, "missing 'groups' key")
        groups = data["groups"]
        self.assertIsInstance(groups, list)
        self.assertGreater(len(groups), 0)

        all_alerts = []
        for group in groups:
            self.assertIn("rules", group)
            for rule in group["rules"]:
                all_alerts.append(rule.get("alert", ""))

        expected = [
            "OF_ExecHealth_SLO_ExporterStale_Warn",
            "OF_ExecHealth_RolloutDrift_Warn",
            "OF_ExecHealth_RolloutDrift_Crit",
            "OF_ExecHealth_CrossScopeModeMismatch_Crit",
            "OF_ExecHealth_CrossScopeThresholdMismatch_Warn",
            "OF_ExecHealth_VetoShareHigh_Warn",
            "OF_ExecHealth_VetoShareHigh_Crit",
        ]
        for name in expected:
            self.assertIn(name, all_alerts, f"missing alert: {name}")

    def test_all_alerts_have_severity_label(self):
        path = self._get_path()
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        for group in data.get("groups", []):
            for rule in group.get("rules", []):
                self.assertIn("severity", rule.get("labels", {}), f"alert {rule.get('alert')} missing severity label")


if __name__ == "__main__":
    unittest.main()
