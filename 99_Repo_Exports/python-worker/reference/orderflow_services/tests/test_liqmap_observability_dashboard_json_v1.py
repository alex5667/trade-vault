from __future__ import annotations

# A1.1 — Smoke tests for LiqMap Observability Grafana dashboard JSON.
# Validates that both the canonical and mirror copies of the dashboard
# are present, parse, have the correct title, and contain the required
# Prometheus queries for the two panels.

import json
import os
import pytest


def _dashboard_path(tick_flow_full: bool = False) -> str:
    """Resolve path to grafana_liqmap_observability_v1.json.

    tick_flow_full=False → python-worker/orderflow_services/
    tick_flow_full=True  → python-worker/tick_flow_full/orderflow_services/
    """
    base = os.path.dirname(__file__)  # python-worker/orderflow_services/tests
    root = os.path.abspath(os.path.join(base, ".."))
    if tick_flow_full:
        tf_root = os.path.abspath(os.path.join(base, "..", "..", "tick_flow_full", "orderflow_services"))
        return os.path.join(tf_root, "grafana_liqmap_observability_v1.json")
    return os.path.join(root, "grafana_liqmap_observability_v1.json")


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_liqmap_dashboard_json_parses(tick_flow_full: bool):
    """File must exist, parse as JSON, have correct title and ≥2 panels."""
    path = _dashboard_path(tick_flow_full)
    assert os.path.isfile(path), f"File not found: {path}"
    with open(path) as fh:
        doc = json.load(fh)
    assert isinstance(doc, dict), "Top-level JSON must be a dict"
    assert doc.get("title") == "LiqMap Observability (v1)", \
        f"Expected title 'LiqMap Observability (v1)', got: {doc.get('title')}"
    panels = doc.get("panels", [])
    assert len(panels) >= 2, f"Expected ≥2 panels, got {len(panels)}"


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_liqmap_dashboard_contains_expected_queries(tick_flow_full: bool):
    """Dashboard panels must reference both A1.1 Prometheus metrics."""
    path = _dashboard_path(tick_flow_full)
    with open(path) as fh:
        doc = json.load(fh)
    exprs = []
    for panel in doc.get("panels", []):
        for target in panel.get("targets", []) or []:
            if isinstance(target, dict) and target.get("expr"):
                exprs.append(target["expr"])
    joined = "\n".join(exprs)
    assert "liqmap_snapshot_age_ms" in joined, "Missing liqmap_snapshot_age_ms query"
    assert "liqmap_parse_errors_total" in joined, "Missing liqmap_parse_errors_total query"


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_liqmap_dashboard_has_template_variables(tick_flow_full: bool):
    """Dashboard must expose 'symbol' and 'window' template variables."""
    path = _dashboard_path(tick_flow_full)
    with open(path) as fh:
        doc = json.load(fh)
    var_names = {v.get("name") for v in doc.get("templating", {}).get("list", [])}
    assert "symbol" in var_names, f"Missing 'symbol' template variable; found: {var_names}"
    assert "window" in var_names, f"Missing 'window' template variable; found: {var_names}"
