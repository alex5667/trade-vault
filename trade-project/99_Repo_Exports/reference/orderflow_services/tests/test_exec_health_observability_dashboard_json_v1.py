"""Tests for Grafana dashboard JSON (exec_health_observability_v1)."""
import json
import pathlib
import unittest

DASHBOARD_FILE = pathlib.Path(__file__).parent.parent / "grafana" / "exec_health_observability_v1.json"
REQUIRED_METRIC_FRAGMENTS = {
    "exec_health_decision_total",
    "exec_health_reader_errors_total",
    "exec_health_rollup_present",
    "exec_health_rollup_value_bps",
    "exec_health_policy_mode",
    "exec_health_policy_threshold_bps",
    "exec_health_tighten_add_bps_scoped",
    "exec_health_flag_total",
}


class TestExecHealthDashboardJSON(unittest.TestCase):
    def setUp(self):
        if not DASHBOARD_FILE.exists():
            self.skipTest(f"Dashboard file not found: {DASHBOARD_FILE}")
        with open(DASHBOARD_FILE) as f:
            self.data = json.load(f)

    def test_valid_json_with_uid(self):
        self.assertIn("uid", self.data)
        self.assertIn("title", self.data)

    def test_panels_present(self):
        panels = self.data.get("panels", [])
        self.assertGreater(len(panels), 0, "No panels in dashboard")

    def test_required_metrics_in_expressions(self):
        dashboard_str = json.dumps(self.data)
        missing = {m for m in REQUIRED_METRIC_FRAGMENTS if m not in dashboard_str}
        self.assertFalse(missing, f"Dashboard missing queries for: {missing}")

    def test_template_variables_present(self):
        templating = self.data.get("templating", {})
        variables = {v["name"] for v in templating.get("list", [])}
        self.assertIn("scope", variables, "Missing 'scope' template variable")
        self.assertIn("symbol", variables, "Missing 'symbol' template variable")


if __name__ == "__main__":
    unittest.main()
