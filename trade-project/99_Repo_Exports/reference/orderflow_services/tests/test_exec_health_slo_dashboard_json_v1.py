from __future__ import annotations
"""Tests for grafana/exec_health_slo_v1.json (P4)."""
import json
import os
import unittest


class TestExecHealthSloDashboardJson(unittest.TestCase):
    def _get_path(self) -> str:
        candidates = [
            os.path.join(os.path.dirname(__file__), "..", "grafana", "exec_health_slo_v1.json"),
            "orderflow_services/grafana/exec_health_slo_v1.json",
            "python-worker/orderflow_services/grafana/exec_health_slo_v1.json",
        ]
        for p in candidates:
            if os.path.isfile(p):
                return os.path.abspath(p)
        raise unittest.SkipTest("exec_health_slo_v1.json not found")

    def test_dashboard_parses_and_has_panels(self):
        path = self._get_path()
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        self.assertIn("panels", data, "missing panels")
        panels = data["panels"]
        self.assertIsInstance(panels, list)
        self.assertGreater(len(panels), 0)

    def test_dashboard_has_required_queries(self):
        path = self._get_path()
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        targets_exprs = []
        for panel in data.get("panels", []):
            for target in panel.get("targets", []):
                targets_exprs.append(target.get("expr", ""))

        combined = "\n".join(targets_exprs)
        required_fragments = [
            "exec_health_slo_active_instances",
            "exec_health_slo_share",
            "exec_health_slo_rollout_drift_instances",
        ]
        for frag in required_fragments:
            self.assertIn(frag, combined, f"missing metric in dashboard targets: {frag}")

    def test_dashboard_has_template_variables(self):
        path = self._get_path()
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        tmpl = data.get("templating", {})
        var_names = [v.get("name", "") for v in tmpl.get("list", [])]
        self.assertIn("scope", var_names, "missing 'scope' template variable")


if __name__ == "__main__":
    unittest.main()
