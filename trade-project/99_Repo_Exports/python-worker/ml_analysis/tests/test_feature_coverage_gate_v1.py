from __future__ import annotations

from ml_analysis.tools import feature_coverage_gate_v1 as gate


def _rows(schema: str = "v3"):
    fkeys = [c[2:] for c in gate.feature_cols_for_schema(schema) if c.startswith("f_")]
    rows = []
    for i in range(3):
        indicators = {k: float(i + 1) for k in fkeys}
        rows.append({"feature_schema_version": schema, "indicators": indicators, "y": 1})
    return rows


def test_gate_passes_when_required_v3_features_present():
    report = gate.evaluate_rows(
        _rows("v3"),
        feature_schema_ver="v3",
        min_present_rate=1.0,
        critical_features=[],
        min_nonzero_sample_n=3,
        fail_on_mixed_schema=True,
    )
    assert report["ok"] is True
    assert report["violations"] == []


def test_gate_fails_missing_required_feature():
    rows = _rows("v3")
    for r in rows:
        r["indicators"].pop("delta_z")
    report = gate.evaluate_rows(
        rows,
        feature_schema_ver="v3",
        min_present_rate=1.0,
        critical_features=[],
        min_nonzero_sample_n=3,
        fail_on_mixed_schema=True,
    )
    assert report["ok"] is False
    assert any(v["kind"] == "low_present_rate" and v["feature"] == "delta_z" for v in report["violations"])


def test_gate_fails_critical_all_zero():
    rows = _rows("v3")
    for r in rows:
        r["indicators"]["delta_z"] = 0.0
    report = gate.evaluate_rows(
        rows,
        feature_schema_ver="v3",
        min_present_rate=1.0,
        critical_features=["delta_z"],
        min_nonzero_sample_n=3,
        fail_on_mixed_schema=True,
    )
    assert report["ok"] is False
    assert any(v["kind"] == "critical_all_zero" and v["feature"] == "delta_z" for v in report["violations"])


def test_gate_fails_mixed_schema_versions():
    rows = _rows("v3")
    rows[0]["feature_schema_version"] = "v4"
    report = gate.evaluate_rows(
        rows,
        feature_schema_ver="v3",
        min_present_rate=1.0,
        critical_features=[],
        min_nonzero_sample_n=3,
        fail_on_mixed_schema=True,
    )
    assert report["ok"] is False
    assert any(v["kind"] == "mixed_feature_schema_version" for v in report["violations"])


def test_gate_default_critical_features_catch_all_zero_any_schema_feature():
    rows = _rows("v3")
    for r in rows:
        r["indicators"]["delta_z"] = 0.0
    report = gate.evaluate_rows(
        rows,
        feature_schema_ver="v3",
        min_present_rate=1.0,
        critical_features=[],
        min_nonzero_sample_n=3,
        fail_on_mixed_schema=True,
    )
    assert report["ok"] is False
    fkeys = [c[2:] for c in gate.feature_cols_for_schema("v3") if c.startswith("f_")]
    assert set(report["critical_features"]) == set(fkeys)
    assert any(v["kind"] == "critical_all_zero" and v["feature"] == "delta_z" for v in report["violations"])


def test_gate_allows_disabling_default_critical_features():
    rows = _rows("v3")
    for r in rows:
        r["indicators"]["delta_z"] = 0.0
    report = gate.evaluate_rows(
        rows,
        feature_schema_ver="v3",
        min_present_rate=1.0,
        critical_features=["none"],
        min_nonzero_sample_n=3,
        fail_on_mixed_schema=True,
    )
    assert report["ok"] is True
    assert report["critical_features"] == []


def test_gate_explicit_all_marks_every_schema_feature_critical():
    rows = _rows("v3")
    fkeys = [c[2:] for c in gate.feature_cols_for_schema("v3") if c.startswith("f_")]
    target = fkeys[-1]
    for r in rows:
        r["indicators"][target] = 0.0
    report = gate.evaluate_rows(
        rows,
        feature_schema_ver="v3",
        min_present_rate=1.0,
        critical_features=["__all__"],
        min_nonzero_sample_n=3,
        fail_on_mixed_schema=True,
    )
    assert report["ok"] is False
    assert set(report["critical_features"]) == set(fkeys)
    assert any(v["kind"] == "critical_all_zero" and v["feature"] == target for v in report["violations"])
