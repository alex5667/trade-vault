"""Tests for Prometheus alert YAML (exec_health_observability_v1)."""
import pathlib
import unittest

import yaml

ALERTS_FILE = pathlib.Path(__file__).parent.parent / "prometheus_alerts_exec_health_observability_v1.yml"
REQUIRED_ALERTS = {
    "OF_ExecHealth_ReaderErrors_Warn",
    "OF_ExecHealth_ReaderErrors_Crit",
    "OF_ExecHealth_VetoStorm_Warn",
    "OF_ExecHealth_VetoStorm_Crit",
    "OF_ExecHealth_RollupsMissing_Warn",
    "OF_ExecHealth_RollupsMissing_Crit",
}


class TestExecHealthAlertsYAML(unittest.TestCase):
    def setUp(self):
        if not ALERTS_FILE.exists():
            self.skipTest(f"Alerts file not found: {ALERTS_FILE}")
        with open(ALERTS_FILE) as f:
            self.data = yaml.safe_load(f)

    def test_groups_present(self):
        self.assertIn("groups", self.data, "top-level 'groups' key missing")
        self.assertGreater(len(self.data["groups"]), 0)

    def test_all_required_alerts_present(self):
        found = set()
        for group in self.data.get("groups", []):
            for rule in group.get("rules", []):
                if "alert" in rule:
                    found.add(rule["alert"])
        missing = REQUIRED_ALERTS - found
        self.assertFalse(missing, f"Missing alerts: {missing}")

    def test_each_alert_has_required_fields(self):
        for group in self.data.get("groups", []):
            for rule in group.get("rules", []):
                if "alert" not in rule:
                    continue
                name = rule["alert"]
                self.assertIn("expr", rule, f"Alert {name} missing 'expr'")
                self.assertIn("for", rule, f"Alert {name} missing 'for'")
                self.assertIn("labels", rule, f"Alert {name} missing 'labels'")
                self.assertIn("annotations", rule, f"Alert {name} missing 'annotations'")
                self.assertIn("severity", rule["labels"], f"Alert {name} missing severity label")

    def test_expr_references_observability_metric(self):
        good_metrics = {
            "exec_health_reader_errors_total",
            "exec_health_decision_total",
            "exec_health_rollup_present",
            "exec_health_last_event_ts_ms",
        }
        for group in self.data.get("groups", []):
            for rule in group.get("rules", []):
                if "alert" in rule:
                    expr = rule.get("expr", "")
                    self.assertTrue(
                        any(m in expr for m in good_metrics),
                        f"Alert {rule['alert']} expr does not reference observability metric",
                    )


if __name__ == "__main__":
    unittest.main()
