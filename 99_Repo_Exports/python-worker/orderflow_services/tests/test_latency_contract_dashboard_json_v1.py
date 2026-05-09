from __future__ import annotations

"""Tests that grafana/latency_contract_v1.json is valid JSON and has expected panels."""

import json
import os

DASHBOARD_FILE = os.path.join(
    os.path.dirname(__file__), "..", "grafana", "latency_contract_v1.json"
)


def test_dashboard_json_is_valid():
    with open(DASHBOARD_FILE, encoding="utf-8") as f:
        data = json.load(f)
    assert data is not None


def test_dashboard_has_title():
    with open(DASHBOARD_FILE, encoding="utf-8") as f:
        data = json.load(f)
    assert "title" in data
    assert data["title"]


def test_dashboard_has_panels():
    with open(DASHBOARD_FILE, encoding="utf-8") as f:
        data = json.load(f)
    assert "panels" in data
    assert isinstance(data["panels"], list)
    assert len(data["panels"]) > 0


def test_dashboard_panels_have_targets():
    with open(DASHBOARD_FILE, encoding="utf-8") as f:
        data = json.load(f)
    for panel in data["panels"]:
        targets = panel.get("targets", [])
        assert isinstance(targets, list), f"Panel {panel.get('id')} has non-list targets"
        assert len(targets) > 0, f"Panel {panel.get('title')} has no targets"
