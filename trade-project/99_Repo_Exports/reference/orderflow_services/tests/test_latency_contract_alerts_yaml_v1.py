"""Tests that prometheus_alerts_latency_contract_v1.yml is valid YAML."""
from __future__ import annotations

import os
import yaml


ALERTS_FILE = os.path.join(
    os.path.dirname(__file__), "..", "prometheus_alerts_latency_contract_v1.yml"
)


def test_alerts_yaml_is_valid():
    with open(ALERTS_FILE, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assert data is not None


def test_alerts_yaml_has_groups():
    with open(ALERTS_FILE, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assert "groups" in data
    assert isinstance(data["groups"], list)
    assert len(data["groups"]) > 0


def test_all_alerts_have_required_fields():
    with open(ALERTS_FILE, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    for group in data["groups"]:
        for rule in group.get("rules", []):
            assert "alert" in rule, f"Missing 'alert' in rule: {rule}"
            assert "expr" in rule, f"Missing 'expr' in rule: {rule}"
            assert "labels" in rule, f"Missing 'labels' in rule: {rule}"
            assert "annotations" in rule, f"Missing 'annotations' in rule: {rule}"
