import pytest
import json
from common.of_gate_metrics_contract import validate_of_gate_row, enrich_schema_fields

def test_validate_of_gate_row_ok():
    row = {
        "ts_ms": "1672531200000",
        "symbol": "BTCUSDT",
        "scenario_v4": "some_scenario",
        "ok": "1",
        "ok_soft": "0",
        "missing_legs": "[]"
    }
    ok, err = validate_of_gate_row(row)
    assert ok is True
    assert err == "ok"

def test_validate_of_gate_row_missing_fields():
    row = {
        "ts_ms": "1672531200000",
        "symbol": "BTCUSDT"
    }
    ok, err = validate_of_gate_row(row)
    assert ok is False
    assert err.startswith("missing_")

def test_validate_of_gate_row_ok_soft_implies_not_ok():
    row = {
        "ts_ms": "1672531200000",
        "symbol": "BTCUSDT",
        "scenario_v4": "some_scenario",
        "ok": "1",
        "ok_soft": "1",
        "missing_legs": "[]"
    }
    ok, err = validate_of_gate_row(row)
    assert ok is False
    assert err == "bad_ok_soft_implies_not_ok"

def test_validate_of_gate_row_bad_missing_legs():
    row = {
        "ts_ms": "1672531200000",
        "symbol": "BTCUSDT",
        "scenario_v4": "some_scenario",
        "ok": "1",
        "ok_soft": "0",
        "missing_legs": '{"not": "a list"}'
    }
    ok, err = validate_of_gate_row(row)
    assert ok is False
    assert err == "bad_missing_legs_type"

def test_enrich_schema_fields():
    payload = {"ok": 1, "scenario_v4": "test"}
    enriched = enrich_schema_fields(payload)
    assert enriched["schema_name"] == "of_gate_metrics"
    assert enriched["schema_version"] == "1"
    assert enriched["reason_code"] == "ok"

    payload_veto = {"meta_veto": 1, "ok": 0}
    enriched = enrich_schema_fields(payload_veto)
    assert enriched["reason_code"] == "meta_veto"
