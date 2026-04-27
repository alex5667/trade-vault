import pytest
from core.confirmations_schema_v1 import parse_confirmations_v1

def test_parse_confirmations_v1_strings_only():
    confirmations = ["rsi_agree=1", "div_match=0", "sweep_eqh=1"]
    out = parse_confirmations_v1(confirmations=confirmations, indicators=None)
    
    assert out["conf_rsi_agree"] == 1.0
    assert out["conf_div_match"] == 0.0
    assert out["conf_sweep_eqh"] == 1.0
    # derived sweep_any
    assert out["conf_sweep_any"] == 1.0
    # default missing
    assert out["conf_iceberg_strict"] == 0.0

def test_parse_confirmations_v1_indicators_only():
    indicators = {
        "rsi_agree": 1,
        "sweep_eql": "true",
        "obi_ok": True,
        "garbage_key": 100
    }
    out = parse_confirmations_v1(confirmations=None, indicators=indicators)
    
    assert out["conf_rsi_agree"] == 1.0
    assert out["conf_sweep_eql"] == 1.0
    assert out["conf_sweep_any"] == 1.0
    assert out["conf_obi_stable"] == 1.0
    assert "garbage_key" not in out
    
def test_parse_confirmations_v1_garbage_values():
    indicators = {
        "rsi_agree": None,
        "sweep_eql": "unparseable string",
        "iceberg_strict": {},
        "conf_div_match": "on",
    }
    confirmations = ["weak_progress=garbo", "reclaim="]
    out = parse_confirmations_v1(confirmations=confirmations, indicators=indicators)
    
    assert out["conf_rsi_agree"] == 0.0
    assert out["conf_sweep_eql"] == 0.0
    assert out["conf_iceberg_strict"] == 0.0
    assert out["conf_div_match"] == 1.0
    assert out["conf_weak_progress"] == 0.0
    assert out["conf_reclaim"] == 0.0
