from __future__ import annotations
"""
Tests for prometheus_rules_bundle_manifest_v1.yml (v9 patch)

Verifies:
- Both manifests (main + tick_flow_full mirror) parse as valid YAML.
- The `rule_file_globs` key is present and contains expected glob patterns.
- The slippage calibrator health alert YAML is referenced (via glob) and parses correctly.
- The slippage-qa runbook (main + tff mirror) references the calibrator health alert file.

Component: Python (orderflow_services tests)
"""


import os
import re
import yaml
import pytest

# ---------------------------------------------------------------------------
# Helpers to resolve paths relative to this test file
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(__file__)  # .../python-worker/orderflow_services/tests
_OF_SERVICES = os.path.abspath(os.path.join(_HERE, ".."))  # orderflow_services/
_TFF_OF_SERVICES = os.path.abspath(
    os.path.join(_HERE, "..", "..", "tick_flow_full", "orderflow_services")
)


def _manifest_path(tick_flow_full: bool = False) -> str:
    root = _TFF_OF_SERVICES if tick_flow_full else _OF_SERVICES
    return os.path.join(root, "prometheus_rules_bundle_manifest_v1.yml")


def _runbook_path(tick_flow_full: bool = False) -> str:
    root = _TFF_OF_SERVICES if tick_flow_full else _OF_SERVICES
    return os.path.join(root, "runbook_slippage_qa_p77.md")


def _calib_health_alert_path(tick_flow_full: bool = False) -> str:
    root = _TFF_OF_SERVICES if tick_flow_full else _OF_SERVICES
    return os.path.join(root, "prometheus_alerts_slippage_calibrator_health_v1.yml")


# ---------------------------------------------------------------------------
# Bundle manifest tests (both main + tick_flow_full)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tick_flow_full", [False, True], ids=["main", "tff"])
def test_bundle_manifest_exists(tick_flow_full: bool) -> None:
    """prometheus_rules_bundle_manifest_v1.yml must exist in both subtrees."""
    path = _manifest_path(tick_flow_full)
    assert os.path.isfile(path), f"Manifest not found: {path}"


@pytest.mark.parametrize("tick_flow_full", [False, True], ids=["main", "tff"])
def test_bundle_manifest_parses_yaml(tick_flow_full: bool) -> None:
    """Manifest must be valid YAML with a `rule_file_globs` list."""
    path = _manifest_path(tick_flow_full)
    with open(path) as fh:
        doc = yaml.safe_load(fh)
    assert isinstance(doc, dict), f"Expected dict at top level: {path}"
    assert "rule_file_globs" in doc, f"Key 'rule_file_globs' missing in {path}"
    globs = doc["rule_file_globs"]
    assert isinstance(globs, list), f"'rule_file_globs' must be a list: {path}"
    assert len(globs) >= 1, f"'rule_file_globs' must not be empty: {path}"


@pytest.mark.parametrize("tick_flow_full", [False, True], ids=["main", "tff"])
def test_bundle_manifest_contains_orderflow_glob(tick_flow_full: bool) -> None:
    """Both manifests must reference orderflow_services/prometheus_alerts_*.yml."""
    path = _manifest_path(tick_flow_full)
    with open(path) as fh:
        doc = yaml.safe_load(fh)
    globs: list[str] = doc["rule_file_globs"]
    assert any(
        "orderflow_services/prometheus_alerts_" in g for g in globs
    ), f"Expected orderflow_services glob in: {globs}"


def test_bundle_manifest_main_contains_ok_rate_logic() -> None:
    """Main manifest (not tff mirror) should include ok_rate_logic glob."""
    path = _manifest_path(tick_flow_full=False)
    with open(path) as fh:
        doc = yaml.safe_load(fh)
    globs: list[str] = doc["rule_file_globs"]
    assert any("ok_rate_logic" in g for g in globs), (
        f"Main manifest should include ok_rate_logic glob; got: {globs}"
    )


# ---------------------------------------------------------------------------
# Slippage calibrator health alert file
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tick_flow_full", [False, True], ids=["main", "tff"])
def test_calib_health_alert_file_exists(tick_flow_full: bool) -> None:
    """prometheus_alerts_slippage_calibrator_health_v1.yml must exist in both subtrees."""
    path = _calib_health_alert_path(tick_flow_full)
    assert os.path.isfile(path), f"Alert file not found: {path}"


@pytest.mark.parametrize("tick_flow_full", [False, True], ids=["main", "tff"])
def test_calib_health_alert_parses_yaml(tick_flow_full: bool) -> None:
    """Alert file must be valid Prometheus rule YAML with at least one alert rule."""
    path = _calib_health_alert_path(tick_flow_full)
    with open(path) as fh:
        doc = yaml.safe_load(fh)
    assert isinstance(doc, dict), f"Top level must be dict: {path}"
    assert "groups" in doc, f"'groups' key missing: {path}"
    all_rules = [r for g in doc["groups"] for r in g.get("rules", [])]
    alert_names = [r.get("alert") for r in all_rules if "alert" in r]
    assert len(alert_names) >= 1, f"No alert rules found in {path}"


# ---------------------------------------------------------------------------
# Runbook update verification
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tick_flow_full", [False, True], ids=["main", "tff"])
def test_runbook_references_calib_health_alert(tick_flow_full: bool) -> None:
    """runbook_slippage_qa_p77.md must reference the calibrator health alert file."""
    path = _runbook_path(tick_flow_full)
    assert os.path.isfile(path), f"Runbook not found: {path}"
    with open(path) as fh:
        content = fh.read()
    assert "prometheus_alerts_slippage_calibrator_health_v1.yml" in content, (
        f"Runbook does not reference calibrator health alert: {path}"
    )


@pytest.mark.parametrize("tick_flow_full", [False, True], ids=["main", "tff"])
def test_runbook_contains_quick_db_check(tick_flow_full: bool) -> None:
    """Runbook must contain the quick SQL triage block for v_exec_slippage_eval."""
    path = _runbook_path(tick_flow_full)
    with open(path) as fh:
        content = fh.read()
    assert "v_exec_slippage_eval" in content, (
        f"Runbook does not contain v_exec_slippage_eval SQL check: {path}"
    )
    assert "exec_regime_bucket" in content, (
        f"Runbook does not contain exec_regime_bucket column reference: {path}"
    )


@pytest.mark.parametrize("tick_flow_full", [False, True], ids=["main", "tff"])
def test_runbook_contains_redis_calib_keys(tick_flow_full: bool) -> None:
    """Runbook must document the Redis calibrator state keys for fast triage."""
    path = _runbook_path(tick_flow_full)
    with open(path) as fh:
        content = fh.read()
    # These keys are written by nightly_slippage_calibrator_v1.py
    assert "state:slippage_calib:last_ok_ts_ms" in content, (
        f"Runbook missing Redis key state:slippage_calib:last_ok_ts_ms: {path}"
    )
    assert "cfg:slippage_decomp_impact_coeff_bps_ts_ms" in content, (
        f"Runbook missing Redis key cfg:slippage_decomp_impact_coeff_bps_ts_ms: {path}"
    )
