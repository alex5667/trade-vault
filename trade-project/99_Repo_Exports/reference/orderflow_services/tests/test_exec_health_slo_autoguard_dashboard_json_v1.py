"""
test_exec_health_slo_autoguard_dashboard_json_v1.py
Validates that the P5 AutoGuard Grafana dashboard JSON is loadable and contains expected queries.
Parametrized: checks both python-worker/orderflow_services/grafana/ copy and root orderflow_services/grafana/.
"""
from __future__ import annotations

import json
import os
import pytest


def _dashboard_path(use_root: bool = False) -> str:
    base = os.path.dirname(__file__)
    if use_root:
        tf_root = os.path.abspath(os.path.join(base, "..", "..", "..", "orderflow_services", "grafana"))
        return os.path.join(tf_root, "exec_health_slo_autoguard_v1.json")
    root = os.path.abspath(os.path.join(base, "..", "grafana"))
    return os.path.join(root, "exec_health_slo_autoguard_v1.json")


@pytest.mark.parametrize("use_root", [False, True])
def test_exec_health_slo_autoguard_dashboard_parses(use_root: bool):
    with open(_dashboard_path(use_root)) as fh:
        doc = json.load(fh)
    assert doc.get("title") == "ExecHealth AutoGuard (v1)"
    assert len(doc.get("panels", [])) >= 3


@pytest.mark.parametrize("use_root", [False, True])
def test_exec_health_slo_autoguard_dashboard_contains_expected_queries(use_root: bool):
    with open(_dashboard_path(use_root)) as fh:
        doc = json.load(fh)
    exprs = []
    for panel in doc.get("panels", []):
        for target in panel.get("targets", []) or []:
            if isinstance(target, dict) and target.get("expr"):
                exprs.append(target["expr"])
    joined = "\n".join(exprs)
    assert "exec_health_slo_autoguard_freeze_active" in joined
    assert "exec_health_slo_autoguard_mode_mismatch_active" in joined
    assert "exec_health_slo_autoguard_rollback_total" in joined
