from __future__ import annotations

# A1.1 — Smoke tests for LiqMap observability Prometheus alert rules.
# Validates that both the canonical copy (orderflow_services/) and the
# mirror copy (tick_flow_full/orderflow_services/) are present, parse as
# valid YAML, and contain all expected alert names.
import os

import pytest
import yaml


def _alerts_path(tick_flow_full: bool = False) -> str:
    """Resolve path to prometheus_alerts_liqmap_observability_v1.yml.

    tick_flow_full=False → python-worker/orderflow_services/
    tick_flow_full=True  → python-worker/tick_flow_full/orderflow_services/
    """
    base = os.path.dirname(__file__)  # python-worker/orderflow_services/tests
    root = os.path.abspath(os.path.join(base, ".."))
    if tick_flow_full:
        tf_root = os.path.abspath(os.path.join(base, "..", "..", "tick_flow_full", "orderflow_services"))
        return os.path.join(tf_root, "prometheus_alerts_liqmap_observability_v1.yml")
    return os.path.join(root, "prometheus_alerts_liqmap_observability_v1.yml")


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_liqmap_observability_alerts_yaml_parses(tick_flow_full: bool):
    """File must exist and be valid YAML with at least one alert group."""
    path = _alerts_path(tick_flow_full)
    assert os.path.isfile(path), f"File not found: {path}"
    with open(path) as fh:
        doc = yaml.safe_load(fh)
    assert isinstance(doc, dict), "Top-level document must be a dict"
    assert "groups" in doc, "YAML must have 'groups' key"
    assert len(doc["groups"]) >= 1, "Must have at least one alert group"
    rules = doc["groups"][0].get("rules", [])
    assert len(rules) > 0, "Alert group must have at least one rule"


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_liqmap_observability_alerts_contains_expected_alerts(tick_flow_full: bool):
    """All four A1.1 alert names must be present."""
    path = _alerts_path(tick_flow_full)
    with open(path) as fh:
        doc = yaml.safe_load(fh)
    rules = doc["groups"][0]["rules"]
    names = [r.get("alert") for r in rules if "alert" in r]
    expected = {
        "OF_LiqMap_SnapshotAgeHigh_Warn",
        "OF_LiqMap_SnapshotAgeHigh_Crit",
        "OF_LiqMap_ParseErrorsHigh_Warn",
        "OF_LiqMap_ParseErrorsHigh_Crit",
    }
    missing = expected - set(names)
    assert not missing, f"Missing alerts: {sorted(missing)}; found={names}"


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_liqmap_observability_alerts_have_required_fields(tick_flow_full: bool):
    """Each rule must have expr, for, labels, and annotations."""
    path = _alerts_path(tick_flow_full)
    with open(path) as fh:
        doc = yaml.safe_load(fh)
    rules = doc["groups"][0]["rules"]
    for rule in rules:
        name = rule.get("alert", "<unnamed>")
        assert "expr" in rule, f"Alert {name}: missing 'expr'"
        assert "for" in rule, f"Alert {name}: missing 'for'"
        assert "labels" in rule, f"Alert {name}: missing 'labels'"
        assert "annotations" in rule, f"Alert {name}: missing 'annotations'"
        assert rule["labels"].get("severity") in ("warning", "critical"), \
            f"Alert {name}: severity must be warning or critical"
