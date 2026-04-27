import pytest
from tools.of_gate_metrics_contract import is_gate_row, derive_ok_fields, scenario_key

def test_is_gate_row():
    assert is_gate_row({"type": "of_gate"})
    assert is_gate_row({"type": "of_gate_metrics_v1"})
    assert is_gate_row({"ok": "1"}) # fallback
    assert not is_gate_row({"type": "other"})
    assert not is_gate_row({})

def test_derive_ok_fields():
    # Only ok
    ok, soft, ok_src, soft_src = derive_ok_fields({"ok": 1})
    assert ok == 1
    assert soft == 0
    assert ok_src == "missing"
    assert soft_src == "missing"

    # With ok_src and ok_soft_src
    ok, soft, ok_src, soft_src = derive_ok_fields({
        "ok": "1", "ok_soft": "0", "ok_src": "ofc.ok", "ok_soft_src": "ev.ok_soft"
    })
    assert ok == 1
    assert soft == 0
    assert ok_src == "ofc.ok"
    assert soft_src == "ev.ok_soft"
    
    # Broken floats
    ok, soft, ok_src, soft_src = derive_ok_fields({"ok": "bad"})
    assert ok == 0
    assert ok_src == "parse_error"

    ok, soft, ok_src, soft_src = derive_ok_fields({"ok": "1.0", "ok_soft": "1.0"})
    assert ok == 1
    assert soft == 1

def test_scenario_key():
    assert scenario_key({"scenario": "test_scenario"}) == "test_scenario"
    assert scenario_key({}) == "na"
