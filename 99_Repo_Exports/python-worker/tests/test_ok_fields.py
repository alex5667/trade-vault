import pytest
from core.ok_fields import parse_ok_fields, get_scenario, get_ts_ms, intish

def test_intish():
    assert intish(None, 42) == 42
    assert intish(True) == 1
    assert intish(False) == 0
    assert intish(5) == 5
    assert intish(5.6) == 5
    assert intish("123") == 123
    assert intish("  45  ") == 45
    assert intish("ok=1") == 1
    assert intish("rule_ok=0") == 0
    assert intish("true") == 1
    assert intish("False") == 0
    assert intish("Yes") == 1
    assert intish("off") == 0
    assert intish("invalid", 99) == 99
    assert intish(b"10") == 10

def test_get_ts_ms():
    assert get_ts_ms({"ts_ms": 12345}) == 12345
    assert get_ts_ms({"ts": "67890"}) == 67890
    assert get_ts_ms({"timestamp": "111"}) == 111
    assert get_ts_ms({"other": 55}) == 0
    assert get_ts_ms({}) == 0

def test_get_scenario():
    assert get_scenario({"scenario_v4": "scen_4"}) == "scen_4"
    assert get_scenario({"scenario": "scen_3"}) == "scen_3"
    assert get_scenario({"scenario_v4": "scen_4", "scenario": "scen_3"}) == "scen_4"
    assert get_scenario({}) == "na"

def test_parse_ok_fields():
    # Test top-level ok
    assert parse_ok_fields({"ok": 1}) == (1, 0)
    assert parse_ok_fields({"ok_soft": 1}) == (0, 1)
    
    # Test nested in payload
    row = {"payload": '{"rule_ok": 1, "ok_soft": "true"}'}
    assert parse_ok_fields(row) == (1, 1)
    
    # Test nested in evidence
    row = {"evidence": '{"ok_rule": 0, "soft_ok": "1"}'}
    assert parse_ok_fields(row) == (0, 1)

    # Test nested in payload -> rule
    row = {"payload": '{"rule": {"ok_strict": "yes", "rule_ok_soft": "on"}}'}
    assert parse_ok_fields(row) == (1, 1)

    # Test dict instead of json string
    row = {"decision": {"ok": 1, "ok_soft": 0}}
    assert parse_ok_fields(row) == (1, 0)

    # Empty
    assert parse_ok_fields({}) == (0, 0)
