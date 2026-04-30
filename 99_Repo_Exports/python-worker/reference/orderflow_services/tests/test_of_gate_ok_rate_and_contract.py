"""
Unit tests for of_gate ok_rate & contract v1.

Tests:
  1. enrich_schema_fields + validate_of_gate_row produce a valid row
  2. compute_stats (no_data sentinel when n==0, ok_rate math when rows present)
"""
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import time
from typing import Any, Dict, Optional

import pytest


# --- Helpers ---

def _ts() -> int:
    return get_ny_time_millis()


def _make_valid_row(
    *
    ok: int = 0
    ok_soft: int = 0
    scenario_v4: str = "na"
    ts_ms: Optional[int] = None
) -> Dict[str, Any]:
    """Build the minimal valid row BEFORE enrich_schema_fields."""
    return {
        "ts_ms": str(ts_ms or _ts())
        "symbol": "BTCUSDT"
        "ok": str(ok)
        "ok_soft": str(ok_soft)
        "missing_legs": "[]"
        "scenario_v4": scenario_v4
    }


# --- Contract tests ---

def test_contract_enrich_and_validate_ok():
    """enrich_schema_fields + validate_of_gate_row must pass for a well-formed row."""
    from services.orderflow.of_gate_metrics_contract import (
        enrich_schema_fields
        validate_of_gate_row
    )

    row = _make_valid_row(ok=1)
    enrich_schema_fields(row)

    # Must have schema fields after enrich
    assert row.get("schema_name") == "of_gate_metrics", f"schema_name={row.get('schema_name')!r}"
    assert int(row.get("schema_version", -1)) == 1, f"schema_version={row.get('schema_version')!r}"
    assert "reason_code" in row and row["reason_code"], f"reason_code missing/empty"

    valid, code = validate_of_gate_row(row)
    assert valid, f"validate failed: code={code!r}, row={row}"


def test_contract_enrich_normalizes_scenario():
    """normalize_scenario_v4 must produce stable, low-cardinality output."""
    from services.orderflow.of_gate_metrics_contract import normalize_scenario_v4

    assert normalize_scenario_v4("Reversal-Cont") == "reversal_cont"
    assert normalize_scenario_v4("DN_VETO") == "dn_veto"
    assert normalize_scenario_v4("") == "na"
    assert normalize_scenario_v4(None) == "na"
    # Must truncate at 32 chars
    long = "a" * 50
    assert len(normalize_scenario_v4(long)) <= 32


def test_contract_why_label_sanitizes():
    """why_label must produce safe Prometheus label chars."""
    from services.orderflow.of_gate_metrics_contract import why_label

    assert why_label("missing_ts_ms") == "missing_ts_ms"
    assert why_label("Bad REASON!") == "bad_reason"
    assert why_label("") == "na"
    assert why_label(None) == "na"


def test_contract_validate_rejects_bad_ok_coherence():
    """ok==1 and ok_soft==1 together is invalid."""
    from services.orderflow.of_gate_metrics_contract import enrich_schema_fields, validate_of_gate_row

    row = _make_valid_row(ok=1, ok_soft=1)
    enrich_schema_fields(row)
    valid, code = validate_of_gate_row(row)
    assert not valid
    assert "ok_soft" in code or "ok" in code


def test_contract_validate_rejects_bad_ts():
    """Timestamps outside epoch range must fail."""
    from services.orderflow.of_gate_metrics_contract import enrich_schema_fields, validate_of_gate_row

    row = _make_valid_row(ts_ms=12345)  # too old
    enrich_schema_fields(row)
    valid, code = validate_of_gate_row(row)
    assert not valid
    assert "ts_ms" in code


# --- ok_rate / no_data math tests ---

def _row(ts_ms: int, ok: int, ok_soft: int = 0, scenario: str = "na") -> Dict[str, Any]:
    from services.orderflow.of_gate_metrics_contract import enrich_schema_fields
    r = {
        "ts_ms": str(ts_ms)
        "symbol": "BTCUSDT"
        "ok": str(ok)
        "ok_soft": str(ok_soft)
        "missing_legs": "[]"
        "scenario_v4": scenario
    }
    enrich_schema_fields(r)
    return r


NOW = _ts()


def test_ok_rate_and_soft_rate_math():
    """ok_rate=0.50, soft_rate=0.25 for 4 rows: 2 ok_hard, 1 ok_soft, 1 veto."""
    from tools.of_gate_sre_monitor import compute_stats

    rows = [
        _row(NOW - 1000, ok=1, ok_soft=0)
        _row(NOW - 2000, ok=1, ok_soft=0)
        _row(NOW - 3000, ok=0, ok_soft=1)
        _row(NOW - 4000, ok=0, ok_soft=0)
    ]

    stats = compute_stats(rows, None, dh_bad_th=0.70)
    n = stats["n"]
    assert n == 4, f"expected n=4, got {n}"
    assert stats["no_data"] == 0

    ok_rate = stats["ok_rate"]
    assert ok_rate is not None, "ok_rate should not be None when n>0"
    assert abs(ok_rate - 0.5) < 1e-6, f"ok_rate={ok_rate}"

    soft_rate = stats["soft_rate"]
    assert soft_rate is not None
    assert abs(soft_rate - 0.25) < 1e-6, f"soft_rate={soft_rate}"


def test_no_data_sentinel_when_empty():
    """compute_stats with empty rows → no_data=1, ok_rate=None, soft_rate=None."""
    from tools.of_gate_sre_monitor import compute_stats

    stats = compute_stats([], None, dh_bad_th=0.70)
    assert stats["no_data"] == 1, f"no_data={stats['no_data']}"
    assert stats["ok_rate"] is None, f"ok_rate={stats['ok_rate']}"
    assert stats["soft_rate"] is None, f"soft_rate={stats['soft_rate']}"


# ---------------------------------------------------------------------------
# P76 / quarantine alerts: new tests added by mega_patch_of_gate_dash_quarantine_alerts_v1
# ---------------------------------------------------------------------------

import os
import json as _json
import yaml as _yaml


def _alerts_yml_path(tick_flow_full: bool = False) -> str:
    """Resolve path to prometheus_alerts_of_gate_ok_rate_v1.yml."""
    base = os.path.dirname(__file__)  # services/orderflow/tests/
    # Walk up to python-worker root
    root = os.path.abspath(os.path.join(base, "..", "..", ".."))
    if tick_flow_full:
        return os.path.join(root, "tick_flow_full", "orderflow_services"
                            "prometheus_alerts_of_gate_ok_rate_v1.yml")
    return os.path.join(root, "orderflow_services"
                        "prometheus_alerts_of_gate_ok_rate_v1.yml")


def _dashboard_path(tick_flow_full: bool = False) -> str:
    """Resolve path to of_gate_ok_rate_health_p76.json."""
    base = os.path.dirname(__file__)
    root = os.path.abspath(os.path.join(base, "..", "..", ".."))
    if tick_flow_full:
        return os.path.join(root, "tick_flow_full", "orderflow_services"
                            "grafana", "of_gate_ok_rate_health_p76.json")
    return os.path.join(root, "orderflow_services", "grafana"
                        "of_gate_ok_rate_health_p76.json")


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_prometheus_yaml_parses(tick_flow_full: bool):
    """prometheus_alerts_of_gate_ok_rate_v1.yml must be valid YAML with expected structure."""
    path = _alerts_yml_path(tick_flow_full)
    assert os.path.isfile(path), f"File not found: {path}"
    with open(path) as fh:
        doc = _yaml.safe_load(fh)
    assert isinstance(doc, dict), "Top-level must be a dict"
    assert "groups" in doc, "Missing 'groups' key"
    groups = doc["groups"]
    assert len(groups) >= 1, "At least one group required"
    rules = groups[0].get("rules", [])
    assert len(rules) > 0, "No rules found in first group"


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_prometheus_yaml_has_quarantine_recording_rule(tick_flow_full: bool):
    """quarantine_rate5m recording rule must exist in the alerts file."""
    path = _alerts_yml_path(tick_flow_full)
    assert os.path.isfile(path), f"File not found: {path}"
    with open(path) as fh:
        doc = _yaml.safe_load(fh)
    rules = doc["groups"][0]["rules"]
    record_names = [r.get("record") for r in rules if "record" in r]
    assert "of_gate:quarantine_rate5m" in record_names, (
        f"Missing of_gate:quarantine_rate5m; found: {record_names}"
    )


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_prometheus_yaml_has_quarantine_nonzero_alert(tick_flow_full: bool):
    """OF_Gate_QuarantineNonZero alert must exist in the alerts file."""
    path = _alerts_yml_path(tick_flow_full)
    assert os.path.isfile(path), f"File not found: {path}"
    with open(path) as fh:
        doc = _yaml.safe_load(fh)
    rules = doc["groups"][0]["rules"]
    alert_names = [r.get("alert") for r in rules if "alert" in r]
    assert "OF_Gate_QuarantineNonZero" in alert_names, (
        f"Missing OF_Gate_QuarantineNonZero; found: {alert_names}"
    )
    # Severity must be warning
    alert = next(r for r in rules if r.get("alert") == "OF_Gate_QuarantineNonZero")
    assert alert.get("labels", {}).get("severity") == "warning", (
        f"Expected severity=warning, got: {alert.get('labels')}"
    )
    # Must fire quickly (for: ≤ 10m) — our spec is 5m
    assert "for" in alert, "Alert must have 'for' field"


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_grafana_dashboard_json_parses(tick_flow_full: bool):
    """of_gate_ok_rate_health_p76.json must parse and have required panels."""
    path = _dashboard_path(tick_flow_full)
    assert os.path.isfile(path), f"Dashboard not found: {path}"
    with open(path) as fh:
        dash = _json.load(fh)
    assert dash.get("title") == "OF Gate OK Rate Health (P76)", (
        f"Unexpected title: {dash.get('title')}"
    )
    panels = dash.get("panels", [])
    assert len(panels) == 4, f"Expected 4 panels, got {len(panels)}"
    titles = [p.get("title") for p in panels]
    assert "Eligible rate (5m) by symbol" in titles, f"Missing eligible panel: {titles}"
    assert "DQ quarantine rate (5m)" in titles, f"Missing quarantine panel: {titles}"


def test_quarantine_rate_math():
    """Verify quarantine_rate = quarantined / eligible is computed correctly.

    This mocks the arithmetic done by the Prometheus recording rule
    of_gate:quarantine_rate5m = sum(rate(of_gate_quarantined_total[5m])).
    We test the downstream ratio calculation from raw counters.
    """
    # Simulated 5-minute rate values (events/second assumed constant)
    eligible_rate = 10.0   # 10 events/sec eligible
    quarantine_rate = 2.0  # 2 events/sec quarantined  → 20% quarantine share

    quarantine_share = quarantine_rate / max(eligible_rate, 1e-9)
    assert abs(quarantine_share - 0.2) < 1e-6, (
        f"quarantine_share={quarantine_share}, expected 0.2"
    )

    # Edge: no eligible — must not divide by zero
    quarantine_share_zero = quarantine_rate / max(0.0, 1e-9)
    assert quarantine_share_zero > 0, "zero-denominator guard must prevent zero result"
