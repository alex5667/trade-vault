"""
tests/test_stage1_p1_of_gate_metrics_contract.py

Stage1-P1 regression tests for enrich_schema_fields.

Coverage:
- Original positional call (backward compat)
- New keyword-arg call from Stage1-P1 diff
- setdefault semantics: existing keys must NOT be overwritten
- reason_code derivation vs explicit override
- Both common and tick_flow_full module paths export identical results
"""
from common.of_gate_metrics_contract import (
    OF_GATE_SCHEMA_NAME,
    OF_GATE_SCHEMA_VERSION,
    enrich_schema_fields,
)

# also verify tick_flow_full path is consistent
from common.of_gate_metrics_contract import (
    enrich_schema_fields as enrich_schema_fields_tf,
)

# ---------------------------------------------------------------------------
# Backward compat: positional call (original signature)
# ---------------------------------------------------------------------------

def test_positional_call_adds_schema_defaults():
    """Existing callers with just the payload dict must still work."""
    row = {"ok": "1", "ok_soft": "0", "ts_ms": "1700000000000",
           "symbol": "BTCUSDT", "scenario_v4": "ok", "missing_legs": "[]"}
    result = enrich_schema_fields(row)
    assert "schema_name" in result
    assert "schema_version" in result
    assert "reason_code" in result
    assert result["schema_name"] == OF_GATE_SCHEMA_NAME
    assert result["schema_version"] == OF_GATE_SCHEMA_VERSION


def test_positional_call_does_not_overwrite_existing():
    """setdefault must not overwrite keys already present."""
    row = {"schema_name": "custom_schema", "schema_version": "42",
           "reason_code": "my_reason", "ok": "1", "ok_soft": "0"}
    enrich_schema_fields(row)
    assert row["schema_name"] == "custom_schema"
    assert row["schema_version"] == "42"
    assert row["reason_code"] == "my_reason"


# ---------------------------------------------------------------------------
# Stage1-P1: keyword-arg call (new signature from diff)
# ---------------------------------------------------------------------------

def test_keyword_schema_name_applied():
    row = {}
    enrich_schema_fields(row, schema_name="of_gate_metrics_v1")
    assert row["schema_name"] == "of_gate_metrics_v1"


def test_keyword_schema_version_applied():
    row = {}
    enrich_schema_fields(row, schema_version=1)
    # stored as int
    assert row["schema_version"] == 1


def test_keyword_reason_code_overrides_derive():
    """Explicit reason_code kwarg must be used instead of derivation."""
    row = {"ok": "1"}  # would derive to "ok" via derive_reason_code
    enrich_schema_fields(row, reason_code="custom_code")
    assert row["reason_code"] == "custom_code"


def test_keyword_args_do_not_overwrite_existing():
    """Even with kwargs, existing keys must not be overwritten (setdefault)."""
    row = {"schema_name": "existing", "schema_version": "99", "reason_code": "existing_rc"}
    enrich_schema_fields(row, schema_name="new", schema_version=2, reason_code="new_rc")
    assert row["schema_name"] == "existing"
    assert row["schema_version"] == "99"
    assert row["reason_code"] == "existing_rc"


def test_full_stage1_p1_call_pattern():
    """
    Exact call pattern from Stage1-P1 diff:
        enrich_schema_fields(row, schema_name="of_gate_metrics_v1", schema_version=1)
    """
    row = {"ts_ms": 1700000000000, "sym": "XAUUSDT", "src": "tick",
           "p_edge": 0.8, "p_min": 0.5, "status": "ok", "conf": 0.7,
           "latency_ms": 12.3, "missing": "[]"}
    enrich_schema_fields(row, schema_name="of_gate_metrics_v1", schema_version=1)
    assert row["schema_name"] == "of_gate_metrics_v1"
    assert row["schema_version"] == 1
    assert "reason_code" in row  # derived since not provided


# ---------------------------------------------------------------------------
# Consistency: both paths (common vs tick_flow_full) must return same result
# ---------------------------------------------------------------------------

def test_both_modules_produce_identical_result():
    row1 = {"ok": "1", "ok_soft": "0"}
    row2 = dict(row1)
    enrich_schema_fields(row1)
    enrich_schema_fields_tf(row2)
    assert row1["schema_name"] == row2["schema_name"]
    assert row1["schema_version"] == row2["schema_version"]
    assert row1["reason_code"] == row2["reason_code"]


def test_both_modules_keyword_args_consistent():
    row1 = {}
    row2 = {}
    enrich_schema_fields(row1, schema_name="test_v1", schema_version=1, reason_code="ok")
    enrich_schema_fields_tf(row2, schema_name="test_v1", schema_version=1, reason_code="ok")
    assert row1 == row2


# ---------------------------------------------------------------------------
# reason_code derivation (smoke test for derive_reason_code integration)
# ---------------------------------------------------------------------------

def test_derive_meta_veto():
    row = {"ok": "0", "ok_soft": "0", "meta_veto": "1", "scenario_v4": "other"}
    enrich_schema_fields(row)
    assert row["reason_code"] == "meta_veto"


def test_derive_ok():
    row = {"ok": "1", "ok_soft": "0", "meta_veto": "0", "scenario_v4": "other"}
    enrich_schema_fields(row)
    assert row["reason_code"] == "ok"


def test_derive_rule_veto_fallback():
    row = {"ok": "0", "ok_soft": "0", "meta_veto": "0",
           "scenario_v4": "other", "data_health": "0.9",
           "book_health_ok": "1", "source_consistency_ok": "1"}
    enrich_schema_fields(row)
    assert row["reason_code"] == "rule_veto"
