"""test_feature_coverage_gate_v15_smoke.py — audit-2026-05-29 item 5.

Smoke test for ``ml_analysis.tools.feature_coverage_gate_v1`` against the
v15_of registry. The gate is the production guard that fails-fast when a
nightly training run would otherwise produce a model with critical features
silently zero — exactly the bug class that motivated the audit.

Tests cover:
  1. ``feature_cols_for_schema('v15_of')`` returns the same column count as
     the registry spec (≥531 f_* cols) — catches schema-loader regression.
  2. ``evaluate_rows`` reports ``ok=True`` when every row supplies every
     f_* feature with non-zero value (happy path).
  3. ``evaluate_rows`` flags the dataset when a critical feature is
     all-zero (the canonical failure the gate is supposed to catch).
  4. ``evaluate_rows`` flags a mixed-schema dataset when
     ``fail_on_mixed_schema=True``.
"""
from __future__ import annotations

import pytest


def _spec_cols() -> list[str]:
    from core.feature_registry import get_edge_stack_feature_spec
    return list(get_edge_stack_feature_spec("v15_of").feature_cols)


def _f_keys() -> list[str]:
    return [c[2:] for c in _spec_cols() if c.startswith("f_")]


def test_feature_cols_for_schema_v15_matches_registry():
    from ml_analysis.tools.feature_coverage_gate_v1 import feature_cols_for_schema

    gate_cols = feature_cols_for_schema("v15_of")
    spec_cols = _spec_cols()
    assert len(gate_cols) == len(spec_cols), (
        f"gate exposes {len(gate_cols)} cols, registry spec has {len(spec_cols)} — "
        "the gate must consume the same registry the trainer pins."
    )
    # ≥531 f_* cols is the v15_of invariant per `_EXPECTED_KEYS`.
    fkeys = [c for c in gate_cols if c.startswith("f_")]
    assert len(fkeys) >= 531


def test_evaluate_rows_happy_path_returns_ok():
    from ml_analysis.tools.feature_coverage_gate_v1 import evaluate_rows

    fkeys = _f_keys()
    # Build 5 rows where every f_* feature is present with a non-zero value
    # and the feature_schema_version is uniformly v15_of.
    row = {
        "feature_schema_version": "v15_of",
        "indicators": {k: 1.0 for k in fkeys},
        "y": 1,
    }
    rows = [dict(row) for _ in range(5)]

    report = evaluate_rows(
        rows,
        feature_schema_ver="v15_of",
        min_present_rate=1.0,
        critical_features=["__all__"],
        min_nonzero_sample_n=5,
        fail_on_mixed_schema=True,
    )
    assert report["ok"] is True, f"unexpected violations: {report['violations'][:3]}"
    assert report["feature_schema_ver"] == "v15_of"
    assert report["rows"] == 5
    assert report["f_features_n"] == len(fkeys)
    assert report["violations_n"] == 0


def test_evaluate_rows_flags_critical_all_zero():
    """One critical feature with value=0.0 across all rows → critical_all_zero
    violation. This is the canonical bug the gate exists to catch."""
    from ml_analysis.tools.feature_coverage_gate_v1 import evaluate_rows

    fkeys = _f_keys()
    poisoned = fkeys[0]
    # Every row: every feature present, ONE feature constantly 0.0.
    rows = []
    for _ in range(10):
        ind = {k: 1.0 for k in fkeys}
        ind[poisoned] = 0.0
        rows.append({
            "feature_schema_version": "v15_of",
            "indicators": ind,
            "y": 0,
        })

    report = evaluate_rows(
        rows,
        feature_schema_ver="v15_of",
        min_present_rate=1.0,
        critical_features=[poisoned],   # mark exactly this feature critical
        min_nonzero_sample_n=10,
        fail_on_mixed_schema=False,
    )
    assert report["ok"] is False
    kinds = {v.get("kind") for v in report["violations"]}
    assert "critical_all_zero" in kinds, (
        f"expected critical_all_zero violation, got kinds={kinds}"
    )
    # The violating feature must be the one we poisoned.
    czs = [v for v in report["violations"] if v.get("kind") == "critical_all_zero"]
    assert any(v.get("feature") == poisoned for v in czs)


def test_evaluate_rows_flags_mixed_schema_when_enabled():
    """Mixed schema versions across rows → mixed_feature_schema_version
    violation when ``fail_on_mixed_schema=True``."""
    from ml_analysis.tools.feature_coverage_gate_v1 import evaluate_rows

    fkeys = _f_keys()
    ind = {k: 1.0 for k in fkeys}
    rows = [
        {"feature_schema_version": "v15_of", "indicators": ind, "y": 1},
        {"feature_schema_version": "v14_of", "indicators": ind, "y": 1},
    ]
    report = evaluate_rows(
        rows,
        feature_schema_ver="v15_of",
        min_present_rate=1.0,
        critical_features=[],
        min_nonzero_sample_n=2,
        fail_on_mixed_schema=True,
    )
    assert report["ok"] is False
    assert any(v.get("kind") == "mixed_feature_schema_version" for v in report["violations"])


def test_evaluate_rows_min_nonzero_sample_n_gates_critical_check():
    """`critical_all_zero` must NOT fire when the row count is below the
    `min_nonzero_sample_n` floor — otherwise CI runs on tiny fixtures
    would false-trip."""
    from ml_analysis.tools.feature_coverage_gate_v1 import evaluate_rows

    fkeys = _f_keys()
    poisoned = fkeys[0]
    # Only 2 rows, both with poisoned=0 — but min_nonzero_sample_n=10.
    rows = [
        {
            "feature_schema_version": "v15_of",
            "indicators": {**{k: 1.0 for k in fkeys}, poisoned: 0.0},
            "y": 1,
        }
        for _ in range(2)
    ]
    report = evaluate_rows(
        rows,
        feature_schema_ver="v15_of",
        min_present_rate=1.0,
        critical_features=[poisoned],
        min_nonzero_sample_n=10,
        fail_on_mixed_schema=False,
    )
    # No critical_all_zero violation because n<min_nonzero_sample_n.
    kinds = {v.get("kind") for v in report["violations"]}
    assert "critical_all_zero" not in kinds
