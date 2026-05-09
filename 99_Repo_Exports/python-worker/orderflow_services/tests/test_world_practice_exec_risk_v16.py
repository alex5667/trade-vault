from __future__ import annotations

# -*- coding: utf-8 -*-
"""Unit tests for exec-risk observability v16 integration.

Tests cover:
- stuck_exec_pen_zero heuristic logic in smoke-check
- New alert names (OFExecPenaltyP95High, OFSpreadP95High) present in YAML
- Smoke-check compiles without syntax errors
"""

import math
import os
import py_compile
from pathlib import Path

import pytest
import yaml

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _smoke_check_path() -> Path:
    """Path to the smoke-check script in the same package tree."""
    return Path(__file__).resolve().parents[1] / "world_practice_gauges_smoke_check_v1.py"


def _alerts_path(tick_flow_full: bool = False) -> str:
    base = os.path.dirname(__file__)
    root = os.path.abspath(os.path.join(base, ".."))
    if tick_flow_full:
        tf_root = os.path.abspath(
            os.path.join(base, "..", "..", "tick_flow_full", "orderflow_services")
        )
        return os.path.join(tf_root, "prometheus_alerts_world_practice_trackers_v1.yml")
    return os.path.join(root, "prometheus_alerts_world_practice_trackers_v1.yml")


def _safe_float(v, default: float = 0.0) -> float:
    try:
        x = float(v)
        if not math.isfinite(x):
            return default
        return x
    except Exception:
        return default


# ---------------------------------------------------------------------------
# 1. Smoke-check compile test
# ---------------------------------------------------------------------------

def test_world_practice_smoke_check_v16_compiles() -> None:
    """Smoke-check tool compiles without syntax errors after v16 changes."""
    p = _smoke_check_path()
    py_compile.compile(str(p), doraise=True)


# ---------------------------------------------------------------------------
# 2. Alert YAML contains new v16 alert names
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_world_practice_alerts_yaml_v16_alerts_present(tick_flow_full: bool) -> None:
    """Both alert files contain the new OFExecPenaltyP95High and OFSpreadP95High alerts."""
    path = _alerts_path(tick_flow_full)
    assert os.path.isfile(path), f"Alert file not found: {path}"
    with open(path) as fh:
        doc = yaml.safe_load(fh)
    rules = doc["groups"][0]["rules"]
    names = {r.get("alert") for r in rules if "alert" in r}
    assert "OFExecPenaltyP95High" in names, f"Missing OFExecPenaltyP95High in {path}"
    assert "OFSpreadP95High" in names, f"Missing OFSpreadP95High in {path}"


@pytest.mark.parametrize("tick_flow_full", [False, True])
def test_world_practice_alerts_yaml_v16_alert_structure(tick_flow_full: bool) -> None:
    """New alerts have required fields: expr, for, labels.severity, annotations."""
    path = _alerts_path(tick_flow_full)
    with open(path) as fh:
        doc = yaml.safe_load(fh)
    rules = doc["groups"][0]["rules"]
    new_alerts = {r["alert"]: r for r in rules
                  if r.get("alert") in ("OFExecPenaltyP95High", "OFSpreadP95High")}
    assert len(new_alerts) == 2, f"Expected 2 new alerts, got {len(new_alerts)}"
    for name, rule in new_alerts.items():
        assert "expr" in rule, f"{name}: missing expr"
        assert "for" in rule, f"{name}: missing for"
        assert rule.get("labels", {}).get("severity") == "warning", f"{name}: severity should be warning"
        assert "annotations" in rule, f"{name}: missing annotations"
        assert "summary" in rule["annotations"], f"{name}: missing annotations.summary"
        assert "description" in rule["annotations"], f"{name}: missing annotations.description"


# ---------------------------------------------------------------------------
# 3. stuck_exec_pen_zero heuristic logic (unit test — no Redis needed)
# ---------------------------------------------------------------------------

def _apply_stuck_exec_heuristic(
    max_exec_risk_norm: float,
    max_exec_pen: float,
    n_recent: int,
    min_recent: int = 200,
) -> bool:
    """Inline replica of the stuck_exec heuristic from smoke-check."""
    stuck_exec = 0
    if n_recent >= min_recent:
        if max_exec_risk_norm >= 0.10 and max_exec_pen <= 1e-6:
            stuck_exec = 1
    return stuck_exec == 1


@pytest.mark.parametrize("exec_risk_norm, exec_pen, n_recent, expect_stuck", [
    # Enough points, high risk, zero penalty → STUCK
    (0.15, 0.0, 300, True),
    (0.10, 0.0, 200, True),
    (0.50, 1e-9, 500, True),
    # High risk, non-zero penalty → NOT stuck
    (0.20, 0.01, 300, False),
    (0.30, 1e-5, 400, False),
    # Low risk → NOT stuck even if exec_pen is zero
    (0.09, 0.0, 300, False),
    (0.00, 0.0, 300, False),
    # Not enough recent points → NOT stuck
    (0.50, 0.0, 50, False),
    (0.50, 0.0, 199, False),
    # Boundary: exactly n_recent == min_recent
    (0.10, 0.0, 200, True),
    # Boundary: exec_risk_norm just below threshold
    (0.099, 0.0, 300, False),
])
def test_stuck_exec_pen_zero_heuristic(
    exec_risk_norm: float,
    exec_pen: float,
    n_recent: int,
    expect_stuck: bool,
) -> None:
    """stuck_exec_pen_zero is raised iff exec_risk_norm>=0.10, exec_pen<=1e-6, n_recent>=min_recent."""
    result = _apply_stuck_exec_heuristic(
        max_exec_risk_norm=exec_risk_norm,
        max_exec_pen=exec_pen,
        n_recent=n_recent,
    )
    assert result == expect_stuck, (
        f"exec_risk_norm={exec_risk_norm}, exec_pen={exec_pen}, n_recent={n_recent}: "
        f"expected stuck={expect_stuck}, got stuck={result}"
    )


# ---------------------------------------------------------------------------
# 4. Smoke-check output fields include v16 additions
# ---------------------------------------------------------------------------

def test_smoke_check_output_fields_named() -> None:
    """Verify the v16 output field names are documented/present in the smoke-check source."""
    src = _smoke_check_path().read_text()
    for field in (
        "max_exec_risk_norm",
        "max_exec_pen",
        "max_spread_bps",
        "max_expected_slip_eff_bps",
        "stuck_exec",
        "stuck_exec_pen_zero",
    ):
        assert field in src, f"Expected field '{field}' not found in smoke-check source"


def test_smoke_check_key_fields_v16() -> None:
    """Verify v16 exec-risk key fields appear in the smoke-check source."""
    src = _smoke_check_path().read_text()
    for field in (
        "spread_bps_submit",
        "impact_proxy",
        "liq_score",
        "expected_slippage_bps",
        "expected_slippage_decomp_bps",
        "exec_risk_norm",
        "exec_pen",
    ):
        assert field in src, f"Expected key_field '{field}' not found in smoke-check source"
