from __future__ import annotations

import os
import yaml
import pytest


def _alerts_path(tick_flow_full: bool = False) -> str:
    base = os.path.dirname(__file__)  # <repo>/orderflow_services/tests
    root = os.path.abspath(os.path.join(base, ".."))
    if tick_flow_full:
        tf_root = os.path.abspath(os.path.join(base, "..", "..", "tick_flow_full", "orderflow_services"))
        return os.path.join(tf_root, "prometheus_alerts_strict_dq_stream_health_v1.yml")
    return os.path.join(root, "prometheus_alerts_strict_dq_stream_health_v1.yml")


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_strict_dq_alerts_yaml_parses(tick_flow_full: bool):
    path = _alerts_path(tick_flow_full)
    assert os.path.isfile(path), f"File not found: {path}"
    with open(path) as fh:
        doc = yaml.safe_load(fh)
    assert isinstance(doc, dict)
    assert "groups" in doc
    assert len(doc["groups"]) >= 1
    rules = doc["groups"][0].get("rules", [])
    assert len(rules) > 0


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_strict_dq_alerts_contains_expected_alerts(tick_flow_full: bool):
    path = _alerts_path(tick_flow_full)
    with open(path) as fh:
        doc = yaml.safe_load(fh)

    rules = doc["groups"][0]["rules"]
    names = [r.get("alert") for r in rules if "alert" in r]
    expected = {
        "OF_DQ_TickGapP95High_Warn",
        "OF_DQ_TickGapP95High_Crit",
        "OF_DQ_TickGapP95Extreme_Crit",
        "OF_DQ_TickMissingSeqEmaHigh_Warn",
        "OF_DQ_TickMissingSeqEmaHigh_Crit",
        "OF_DQ_BookMissingSeqEmaHigh_Crit",
        "OF_DQ_LevelHardShareHigh_Crit",
    }
    missing = expected - set(names)
    assert not missing, f"Missing alerts: {sorted(missing)}; found={names}"
