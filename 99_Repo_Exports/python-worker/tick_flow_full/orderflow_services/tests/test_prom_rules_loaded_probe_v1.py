from __future__ import annotations
"""Tests for prom_rules_loaded_probe_v1 (v18).

Covers:
  - YAML parsing of alert rules file
  - _extract_loaded_rule_files helper
  - _compute_loaded_expected helper
  - runbook mentions (links P91 section exists)
"""

import json
import os
from pathlib import Path

import pytest
import yaml

from orderflow_services.prom_rules_loaded_probe_v1 import (
    _compute_loaded_expected,
    _extract_loaded_rule_files,
    _get_repo_root,
)

# ── YAML parse: alert rules file ─────────────────────────────────────────────

_HERE = Path(__file__).resolve().parent
_RULES_CANDIDATES = [
    # In orderflow_services tree (default)
    _HERE.parent / "prometheus_alerts_prom_rules_loaded_probe_v1.yml",
    # In tick_flow_full mirrorring
    _HERE.parent.parent / "tick_flow_full" / "orderflow_services" / "prometheus_alerts_prom_rules_loaded_probe_v1.yml",
]


def _get_rules_path() -> Path | None:
    for p in _RULES_CANDIDATES:
        if p.exists():
            return p
    return None


def test_alert_rules_yaml_parseable():
    """Alert rules YAML must be loadable without syntax errors."""
    path = _get_rules_path()
    if path is None:
        pytest.skip("prometheus_alerts_prom_rules_loaded_probe_v1.yml not found")
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    assert isinstance(doc, dict), "Top-level must be a dict"
    assert "groups" in doc, "Missing 'groups' key"


def test_alert_rules_have_groups_and_rules():
    """Ensure at least one group and one rule exist in the alert file."""
    path = _get_rules_path()
    if path is None:
        pytest.skip("prometheus_alerts_prom_rules_loaded_probe_v1.yml not found")
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    groups = doc.get("groups", [])
    assert len(groups) >= 1, "At least one group expected"
    rule_count = sum(len(g.get("rules", [])) for g in groups if isinstance(g, dict))
    assert rule_count >= 1, "At least one rule expected"


def test_alert_rules_contain_expected_alerts():
    """v18 expects P91-A, P91-C and P91-D stall-check alerts."""
    path = _get_rules_path()
    if path is None:
        pytest.skip("prometheus_alerts_prom_rules_loaded_probe_v1.yml not found")
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    alert_names = {
        r.get("alert")
        for g in doc.get("groups", [])
        if isinstance(g, dict)
        for r in g.get("rules", [])
        if isinstance(r, dict) and r.get("alert")
    }
    # Must have the "missing files" critical alert (P91-A)
    assert "OF_PromRulesFilesMissing_Crit" in alert_names, f"Missing P91-A alert. Got: {alert_names}"
    # Must have the stale probe alert (P91-C)
    assert "OF_PromRulesLoadedProbeStale_Warn" in alert_names, f"Missing P91-C stale alert. Got: {alert_names}"
    # Must have the stall-check (P91-D, v18 addition)
    assert "OF_PromRuleGroupEvalStall_Warn" in alert_names, f"Missing P91-D stall-check alert. Got: {alert_names}"


def test_alert_rules_stall_check_refers_to_correct_metric():
    """The stall-check alert must reference prometheus_rule_group_last_evaluation_timestamp_seconds."""
    path = _get_rules_path()
    if path is None:
        pytest.skip("prometheus_alerts_prom_rules_loaded_probe_v1.yml not found")
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    stall_rule = None
    for g in doc.get("groups", []):
        if not isinstance(g, dict):
            continue
        for r in g.get("rules", []):
            if isinstance(r, dict) and r.get("alert") == "OF_PromRuleGroupEvalStall_Warn":
                stall_rule = r
                break
    assert stall_rule is not None, "OF_PromRuleGroupEvalStall_Warn not found"
    assert "prometheus_rule_group_last_evaluation_timestamp_seconds" in stall_rule.get("expr", ""), (
        "Stall-check expr must reference prometheus_rule_group_last_evaluation_timestamp_seconds"
    )


# ── Runbook existence and P91 mention ────────────────────────────────────────

_RUNBOOK_CANDIDATES = [
    _HERE.parent / "runbook_world_practice_trackers_v1.md",
    _HERE.parent.parent / "tick_flow_full" / "orderflow_services" / "runbook_world_practice_trackers_v1.md",
]


def _get_runbook_path() -> Path | None:
    for p in _RUNBOOK_CANDIDATES:
        if p.exists():
            return p
    return None


def test_runbook_exists():
    assert _get_runbook_path() is not None, "runbook_world_practice_trackers_v1.md not found"


def test_runbook_mentions_p91_section():
    """Runbook must have a section describing the rules-loaded probe (P91)."""
    path = _get_runbook_path()
    if path is None:
        pytest.skip("runbook not found")
    content = path.read_text(encoding="utf-8")
    assert "prom_rules_loaded_probe_v1" in content, "Runbook must mention prom_rules_loaded_probe_v1"
    assert "P91" in content, "Runbook must have P91 section"
    # The runbook should mention the stall-check alert
    assert "OF_PromRuleGroupEvalStall_Warn" in content or "stall" in content.lower(), (
        "Runbook should mention the stall-check alert (P91-D)"
    )


# ── _extract_loaded_rule_files ────────────────────────────────────────────────

def test_extract_loaded_rule_files():
    payload = {
        "status": "success",
        "data": {
            "groups": [
                {"file": "/etc/prometheus/rules/bundle/orderflow_services/prometheus_alerts_foo.yml"},
                {"file": "/etc/prometheus/rules/bundle/orderflow_services/prometheus_rules_bar.yml"},
            ]
        },
    }
    loaded = _extract_loaded_rule_files(payload)
    assert len(loaded) == 2
    assert "/etc/prometheus/rules/bundle/orderflow_services/prometheus_alerts_foo.yml" in loaded


def test_extract_loaded_rule_files_empty():
    payload = {"status": "success", "data": {"groups": []}}
    assert len(_extract_loaded_rule_files(payload)) == 0


def test_extract_loaded_rule_files_bad_status():
    with pytest.raises(RuntimeError):
        _extract_loaded_rule_files({"status": "error"})


def test_extract_loaded_rule_files_skips_blank():
    payload = {
        "status": "success",
        "data": {
            "groups": [
                {"file": "   "},
                {"file": "/etc/prometheus/rules/foo.yml"},
            ]
        },
    }
    loaded = _extract_loaded_rule_files(payload)
    assert len(loaded) == 1
    assert "/etc/prometheus/rules/foo.yml" in loaded


# ── _compute_loaded_expected ──────────────────────────────────────────────────

def test_compute_loaded_expected_all_ok():
    expected_rel = [
        "orderflow_services/prometheus_alerts_prom_rules_bundle_health_v1.yml",
        "orderflow_services/prometheus_alerts_prom_rules_loaded_probe_v1.yml",
    ]
    loaded_files = {
        "/etc/prometheus/rules/bundle/orderflow_services/prometheus_alerts_prom_rules_bundle_health_v1.yml",
        "/etc/prometheus/rules/bundle/orderflow_services/prometheus_alerts_prom_rules_loaded_probe_v1.yml",
        "/etc/prometheus/rules/bundle/other_file.yml",
    }
    loaded_n, missing = _compute_loaded_expected(expected_rel=expected_rel, loaded_files=loaded_files)
    assert loaded_n == 2
    assert len(missing) == 0


def test_compute_loaded_expected_one_missing():
    expected_rel = [
        "orderflow_services/prometheus_rules_bundle_manifest_v2.yml",
        "orderflow_services/prometheus_alerts_prom_rules_bundle_health_v1.yml",
    ]
    loaded_files_missing = {
        "/etc/prometheus/rules/bundle/orderflow_services/prometheus_rules_bundle_manifest_v2.yml"
    }
    loaded_n, missing = _compute_loaded_expected(expected_rel=expected_rel, loaded_files=loaded_files_missing)
    assert loaded_n == 1
    assert len(missing) == 1
    assert "orderflow_services/prometheus_alerts_prom_rules_bundle_health_v1.yml" in missing


def test_compute_loaded_expected_all_missing():
    expected_rel = ["orderflow_services/foo.yml", "orderflow_services/bar.yml"]
    loaded_n, missing = _compute_loaded_expected(expected_rel=expected_rel, loaded_files=set())
    assert loaded_n == 0
    assert set(missing) == set(expected_rel)


def test_compute_loaded_expected_empty_expected():
    loaded_n, missing = _compute_loaded_expected(expected_rel=[], loaded_files={"some/file.yml"})
    assert loaded_n == 0
    assert missing == []


# ── _get_repo_root ────────────────────────────────────────────────────────────

def test_get_repo_root_with_explicit_path(tmp_path):
    result = _get_repo_root(str(tmp_path))
    assert result == tmp_path.resolve()


def test_get_repo_root_no_arg():
    """Should not raise and return a Path."""
    result = _get_repo_root(None)
    assert isinstance(result, Path)
