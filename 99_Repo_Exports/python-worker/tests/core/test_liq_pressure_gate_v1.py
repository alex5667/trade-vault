import pytest
from core.liq_pressure_gate_v1 import eval_liq_pressure_gate

def test_eval_liq_pressure_gate_off():
    cfg = {"liq_pressure_gate_mode": "off"}
    res = eval_liq_pressure_gate("LONG", 0.5, 0.5, cfg)
    assert res == (0.0, 0.0, 0, "", 0, 0)

def test_eval_liq_pressure_gate_boost():
    cfg = {
        "liq_pressure_gate_mode": "boost",
        "liq_pressure_qimb_thr": 0.1,
        "liq_pressure_ofi_thr": 0.1,
        "liq_pressure_boost_max": 0.05
    }
    # Both align LONG
    res = eval_liq_pressure_gate("LONG", 0.2, 0.2, cfg)
    boost, pen, veto, reason, qa, oa = res
    assert boost == 0.05
    assert pen == 0.0
    assert veto == 0
    assert "bst" in reason
    assert qa == 1
    assert oa == 1

def test_eval_liq_pressure_gate_penalty():
    cfg = {
        "liq_pressure_gate_mode": "penalty",
        "liq_pressure_qimb_thr": 0.1,
        "liq_pressure_ofi_thr": 0.1,
        "liq_pressure_pen_max": 0.1
    }
    # Contradiction: LONG but qimb negative (Ask heavy)
    res = eval_liq_pressure_gate("LONG", -0.2, 0.2, cfg)
    boost, pen, veto, reason, qa, oa = res
    assert boost == 0.0
    assert pen == 0.05 # 0.5 * pen_max because only one leg failed
    assert veto == 0
    assert "bad_q" in reason
    assert qa == -1
    assert oa == 1

def test_eval_liq_pressure_gate_veto():
    cfg = {
        "liq_pressure_gate_mode": "enforce",
        "liq_pressure_qimb_thr": 0.1,
        "liq_pressure_ofi_thr": 0.1,
        "liq_pressure_veto_mult": 2.0
    }
    # Severe Contradiction: LONG but qimb very negative
    res = eval_liq_pressure_gate("LONG", -0.3, 0.0, cfg)
    boost, pen, veto, reason, qa, oa = res
    assert veto == 1
    assert "VETO" in reason
    assert qa == -1
