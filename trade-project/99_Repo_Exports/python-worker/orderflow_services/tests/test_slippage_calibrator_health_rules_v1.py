from __future__ import annotations

"""Tests for prometheus_alerts_slippage_calibrator_health_v1.yml (V10).

Validates that the alert YAML in both orderflow_services/ and
tick_flow_full/orderflow_services/ trees:
  - parses as valid YAML
  - contains the expected V10 alert names
  - has correct severity labels

Works without a running Prometheus instance.
"""


from pathlib import Path
from typing import Any

import pytest
import yaml

BASE = Path(__file__).parent.parent  # orderflow_services/tests/../ = orderflow_services/


def _alert_yaml_path(tree: str) -> Path:
    if tree == "orderflow_services":
        return BASE / "prometheus_alerts_slippage_calibrator_health_v1.yml"
    elif tree == "tick_flow_full":
        # BASE = orderflow_services/, BASE.parent = python-worker/
        return (
            BASE.parent
            / "tick_flow_full"
            / "orderflow_services"
            / "prometheus_alerts_slippage_calibrator_health_v1.yml"
        )
    raise ValueError(f"Unknown tree: {tree}")


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _get_all_alerts(doc: dict[str, Any]) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    for group in doc.get("groups", []):
        for rule in group.get("rules", []):
            if "alert" in rule:
                alerts.append(rule)
    return alerts


# V10 required alert names
EXPECTED_ALERTS = {
    "OF_SlippageCalibratorStale_Crit",
    "OF_SlippageCalibratorNoUpdates_Warn",
    "OF_SlippageCoeffStaleHVLL_Warn",
    "OF_ExecSlippageEvalRowcountProbeStale_Crit",
    "OF_ExecSlippageEvalRowsLow_Warn",
}


@pytest.mark.parametrize("tree", ["orderflow_services", "tick_flow_full"])
class TestSlippageCalibratorHealthAlerts:
    def _alerts(self, tree: str) -> list[dict[str, Any]]:
        p = _alert_yaml_path(tree)
        doc = _load_yaml(p)
        return _get_all_alerts(doc)

    def test_yaml_parses(self, tree: str) -> None:
        """File must be valid YAML with a 'groups' key."""
        p = _alert_yaml_path(tree)
        assert p.exists(), f"Alert YAML not found: {p}"
        doc = _load_yaml(p)
        assert "groups" in doc, f"'groups' key missing in {p}"
        assert isinstance(doc["groups"], list), f"'groups' must be a list in {p}"

    def test_required_alerts_present(self, tree: str) -> None:
        """All V10 alert names must be present."""
        alerts = self._alerts(tree)
        found = {a["alert"] for a in alerts}
        missing = EXPECTED_ALERTS - found
        assert not missing, (
            f"Alert YAML for {tree!r} is missing alerts: {missing}.\n"
            f"Found: {found}"
        )

    def test_no_duplicate_alert_names(self, tree: str) -> None:
        """Alert names must be unique within the file."""
        alerts = self._alerts(tree)
        names = [a["alert"] for a in alerts]
        seen: dict[str, int] = {}
        for name in names:
            seen[name] = seen.get(name, 0) + 1
        duplicates = {k: v for k, v in seen.items() if v > 1}
        assert not duplicates, f"Duplicate alert names in {tree}: {duplicates}"

    def test_stale_crit_has_critical_severity(self, tree: str) -> None:
        """StaleProbe and CalibStale should be critical."""
        alerts = {a["alert"]: a for a in self._alerts(tree)}
        for name in ("OF_SlippageCalibratorStale_Crit", "OF_ExecSlippageEvalRowcountProbeStale_Crit"):
            assert name in alerts, f"{name} not found in {tree}"
            sev = alerts[name].get("labels", {}).get("severity", "")
            assert sev == "critical", f"{name} in {tree}: expected severity=critical, got {sev!r}"

    def test_warn_alerts_have_warning_severity(self, tree: str) -> None:
        """Warn alerts must have severity=warning."""
        alerts = {a["alert"]: a for a in self._alerts(tree)}
        for name in (
            "OF_SlippageCalibratorNoUpdates_Warn",
            "OF_SlippageCoeffStaleHVLL_Warn",
            "OF_ExecSlippageEvalRowsLow_Warn",
        ):
            assert name in alerts, f"{name} not found in {tree}"
            sev = alerts[name].get("labels", {}).get("severity", "")
            assert sev == "warning", f"{name} in {tree}: expected severity=warning, got {sev!r}"

    def test_all_alerts_have_expr(self, tree: str) -> None:
        """Every alert rule must have an expr field."""
        alerts = self._alerts(tree)
        for a in alerts:
            assert "expr" in a, f"Alert {a['alert']!r} in {tree} is missing 'expr'"

    def test_all_alerts_have_annotations(self, tree: str) -> None:
        """Every alert must have summary + description annotations."""
        alerts = self._alerts(tree)
        for a in alerts:
            ann = a.get("annotations", {})
            name = a["alert"]
            assert "summary" in ann, f"Alert {name!r} in {tree} missing 'summary' annotation"
            assert "description" in ann, f"Alert {name!r} in {tree} missing 'description' annotation"

    def test_rowcount_probe_stale_uses_age_gauge(self, tree: str) -> None:
        """OF_ExecSlippageEvalRowcountProbeStale_Crit must reference the age gauge."""
        alerts = {a["alert"]: a for a in self._alerts(tree)}
        a = alerts["OF_ExecSlippageEvalRowcountProbeStale_Crit"]
        expr = a["expr"]
        assert "of_exec_slippage_eval_rows_24h_age_sec" in expr, (
            f"OF_ExecSlippageEvalRowcountProbeStale_Crit expr should reference "
            f"of_exec_slippage_eval_rows_24h_age_sec, got: {expr!r}"
        )

    def test_rows_low_uses_sum_of_rows_gauge(self, tree: str) -> None:
        """OF_ExecSlippageEvalRowsLow_Warn must reference the rows bucket gauge."""
        alerts = {a["alert"]: a for a in self._alerts(tree)}
        a = alerts["OF_ExecSlippageEvalRowsLow_Warn"]
        expr = a["expr"]
        assert "of_exec_slippage_eval_rows_24h" in expr, (
            f"OF_ExecSlippageEvalRowsLow_Warn expr should reference "
            f"of_exec_slippage_eval_rows_24h, got: {expr!r}"
        )
