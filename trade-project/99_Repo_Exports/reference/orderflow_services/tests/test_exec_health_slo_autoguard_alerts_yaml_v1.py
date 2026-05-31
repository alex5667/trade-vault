"""
test_exec_health_slo_autoguard_alerts_yaml_v1.py
Validates that the P5 AutoGuard Prometheus alerts YAML is loadable and contains expected alert names.
Parametrized: checks both the python-worker/orderflow_services copy and the root orderflow_services copy.
"""
from __future__ import annotations

import os
import yaml
import pytest


def _alerts_path(tick_flow_full: bool = False) -> str:
    base = os.path.dirname(__file__)
    root = os.path.abspath(os.path.join(base, ".."))
    if tick_flow_full:
        tf_root = os.path.abspath(os.path.join(base, "..", "..", "..", "orderflow_services"))
        return os.path.join(tf_root, "prometheus_alerts_exec_health_slo_autoguard_v1.yml")
    return os.path.join(root, "prometheus_alerts_exec_health_slo_autoguard_v1.yml")


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_exec_health_slo_autoguard_alerts_yaml_parses(tick_flow_full: bool):
    with open(_alerts_path(tick_flow_full)) as fh:
        doc = yaml.safe_load(fh)
    assert isinstance(doc, dict)
    assert len(doc.get("groups", [])) >= 1


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_exec_health_slo_autoguard_alerts_have_expected_names(tick_flow_full: bool):
    with open(_alerts_path(tick_flow_full)) as fh:
        doc = yaml.safe_load(fh)
    names = {r.get("alert") for r in doc["groups"][0].get("rules", []) if "alert" in r}
    expected = {
        "OF_ExecHealth_AutoGuard_ExporterStale_Warn",
        "OF_ExecHealth_AutoGuard_FreezeActive_Warn",
        "OF_ExecHealth_AutoGuard_RollbackPerformed_Warn",
    }
    assert expected.issubset(names)
