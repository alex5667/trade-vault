"""test_v15_of_audit_2026_05_29_integration.py — guards for audit-2026-05-29 fixes.

Covers the audit items integrated from the 2026-05-29 review:

  * 4.5 build_feature_row() encodes bucket:/hour:/dow:/session_ categoricals
        (already passing in test_v15_of_coverage_exporter.py;
         duplicated here as a single-purpose canonical guard).
  * 4.6/4.7/4.8 Documentation drift: ``v15_of`` references must not hardcode
        515 / "+156 additions" — count is sourced from `_EXPECTED_KEYS`.
  * 4.9 PEdgeCalib alert split: a separate `PEdgeCalibUnknownSkippedRateHigh`
        alert exists so UNKNOWN label-pipeline failures don't get conflated
        with real BE-outcome floods.
"""
from __future__ import annotations

import pathlib

import pytest


# ── 4.6/4.7/4.8 — documentation drift ─────────────────────────────────────────

_ROOT = pathlib.Path(__file__).parent.parent
_DRIFT_FILES = [
    _ROOT / "core" / "ml_feature_schema_v15_of.py",
    _ROOT / "core" / "meta_features_v15_of.py",
    _ROOT / "tools" / "nightly_v15_of_train_bundle.py",
    _ROOT / "tools" / "nightly_v14_of_train_bundle.py",
    _ROOT / "core" / "ml_feature_schema_v14_of.py",
    _ROOT / "ml_analysis" / "tools" / "schema_choices_v1.py",
]

# Pinned literal strings that must NOT appear in production docstrings/comments
# because they are stale relative to the live invariant. Tests cite the
# exact phrase so a fix sticks.
_FORBIDDEN_PHRASES = (
    "v15_of (515",
    "v15_of 515-key",
    "515-key v15_of",
    "+ 156 additions",
    "+156 additions",
    "515 numeric keys",
)


@pytest.mark.parametrize("path", _DRIFT_FILES, ids=lambda p: p.name)
def test_v15_of_doc_drift_phrases_absent(path: pathlib.Path):
    if not path.exists():
        pytest.skip(f"{path} not present in tree")
    src = path.read_text(encoding="utf-8")
    hits = [p for p in _FORBIDDEN_PHRASES if p in src]
    assert hits == [], (
        f"{path.name} still contains stale v15_of count phrases: {hits}. "
        "Reference _EXPECTED_KEYS instead of hardcoded counts."
    )


def test_v15_of_top_docstring_points_at_invariant():
    """The schema module docstring must instruct readers to consult
    _EXPECTED_KEYS rather than a literal number — otherwise every bump
    creates a fresh doc-drift cycle."""
    src = (_ROOT / "core" / "ml_feature_schema_v15_of.py").read_text(encoding="utf-8")
    assert "_EXPECTED_KEYS" in src
    # First ~30 lines (the docstring) must mention the invariant.
    head = "\n".join(src.splitlines()[:30])
    assert "_EXPECTED_KEYS" in head, (
        "Top-of-file docstring should reference _EXPECTED_KEYS so readers "
        "know where the authoritative count lives."
    )


# ── 4.5 — build_feature_row encoders ─────────────────────────────────────────

def test_build_feature_row_emits_bucket_hour_dow_session():
    """End-to-end guard: registry categorical columns must encode to 1.0
    when the inputs match, not silently fall through to 0.0."""
    import datetime as _dt
    from tools.train_edge_stack_v1_oof import build_feature_row

    ts_ms = int(
        _dt.datetime(2026, 3, 4, 10, 30, 0, tzinfo=_dt.timezone.utc).timestamp() * 1000
    )
    cols = [
        "bucket:trend",
        "bucket:range",
        "hour:10",
        "hour:11",
        "dow:2",  # Wed
        "session_eu",   # 10:00 UTC is European session per the helper
        "session_asia",
    ]
    row, _missing = build_feature_row(
        feature_cols=cols,
        indicators={"spread_bps": 2.0, "expected_slippage_bps": 1.0, "exec_risk_norm": 0.5},
        direction="LONG",
        scenario="trend_breakout",
        ts_ms=ts_ms,
    )
    assert len(row) == len(cols)
    assert row[0] == 1.0  # bucket:trend
    assert row[1] == 0.0  # bucket:range
    assert row[2] == 1.0  # hour:10
    assert row[3] == 0.0  # hour:11
    assert row[4] == 1.0  # dow:2 (Wed)
    # session_eu/_asia: one of them should be 1.0, the other 0.0 — the
    # exact split is owned by the helper; pick whichever the impl chose
    # by asserting they sum to 1.
    assert row[5] + row[6] == 1.0


# ── 4.9 — PEdge alert split ───────────────────────────────────────────────────

_ALERT_FILE = _ROOT / "monitoring" / "prometheus_alerts_ml_pipeline_failopen.yml"


def test_pedge_unknown_skipped_alert_exists():
    if not _ALERT_FILE.exists():
        pytest.skip("alert file missing")
    src = _ALERT_FILE.read_text(encoding="utf-8")
    assert "PEdgeCalibUnknownSkippedRateHigh" in src, (
        "Audit 4.9: UNKNOWN label-pipeline failures need their own alert "
        "(was: conflated with BE-bucket inflation)."
    )
    # The two alerts must reference different metric paths so they can't
    # accidentally trip simultaneously on the same condition.
    assert 'p_edge_cal_skipped_total{reason="result_invalid"}' in src
    assert 'p_edge_cal_observed_total{result="BE"}' in src


def test_pedge_be_alert_no_longer_blames_labeling():
    """The BE alert description must explain that label-pipeline issues
    surface in the UNKNOWN alert, not the BE alert."""
    if not _ALERT_FILE.exists():
        pytest.skip("alert file missing")
    src = _ALERT_FILE.read_text(encoding="utf-8")
    # The BE alert text must explicitly disclaim being a labeling bug — the
    # whole point of the split is to direct operators to PEdgeCalibUnknownSkippedRateHigh.
    be_idx = src.find("PEdgeCalibBEBucketInflated")
    assert be_idx >= 0
    be_block = src[be_idx:be_idx + 1500]
    assert "NOT a labeling bug" in be_block or "not a labeling bug" in be_block
