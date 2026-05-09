"""Tests for Prometheus alerts YAML for world-practice adverse realized drift v1 - tick_flow_full."""
import os

import yaml


def test_adverse_rd_alerts_yaml_parses():
    path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "prometheus_alerts_world_practice_adverse_rd_v1.yml",
    )
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f)

    assert "groups" in doc
    groups = doc["groups"]
    assert isinstance(groups, list) and groups
    g0 = groups[0]
    assert "rules" in g0 and isinstance(g0["rules"], list)
    names = {r.get("alert") for r in g0["rules"]}
    assert "OF_WP_AdverseRdVeto_Crit" in names
    assert "OF_WP_AdverseRdBadShareHigh_Warn" in names
    assert "OF_WP_AdverseRdWiringStuck_Crit" in names


def test_adverse_rd_dashboard_files_exist():
    base = os.path.join(os.path.dirname(__file__), "..", "..", "orderflow_services", "grafana")
    assert os.path.exists(os.path.join(base, "world_practice_adverse_rd_v1.json"))
    assert os.path.exists(os.path.join(base, "README_world_practice_adverse_rd_v1.md"))
