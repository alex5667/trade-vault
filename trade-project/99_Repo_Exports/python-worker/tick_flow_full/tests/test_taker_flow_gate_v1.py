"""Tests for eval_taker_flow_gate in tick_flow_full (mirrors services/ tests).

Uses tick_flow_full.core.taker_flow_gate_v1 import path.
"""

# Ensure tick_flow_full is importable when running from python-worker dir
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tick_flow_full.core.taker_flow_gate_v1 import eval_taker_flow_gate


def test_taker_flow_gate_rate_too_low():
    cfg2 = {"taker_flow_gate_min_abs_rate": 10.0}
    indicators = {"taker_buy_rate_ema": 2.0, "taker_sell_rate_ema": 3.0}
    res = eval_taker_flow_gate("LONG", indicators, cfg2)
    assert res.veto == 0
    assert res.shadow_veto == 0
    assert res.reason == "low_rate"


def test_taker_flow_gate_pass():
    cfg2 = {"taker_flow_gate_mode": "enforce"}
    indicators = {"taker_flow_imb": 0.1, "taker_flow_imb_z": 1.0, "taker_buy_rate_ema": 100.0}
    res = eval_taker_flow_gate("LONG", indicators, cfg2)
    assert res.veto == 0
    assert res.shadow_veto == 0
    assert res.reason == "ok"


def test_taker_flow_gate_contra_long_enforce():
    cfg2 = {
        "taker_flow_gate_mode": "enforce",
        "taker_flow_contra_imb_hard": 0.2,
        "taker_flow_contra_z_hard": 2.0,
    }
    indicators = {"taker_flow_imb": -0.3, "taker_flow_imb_z": -2.5, "taker_buy_rate_ema": 100.0}
    res = eval_taker_flow_gate("LONG", indicators, cfg2)
    assert res.veto == 1
    assert res.shadow_veto == 0
    assert res.reason == "contra"


def test_taker_flow_gate_shadow_mode():
    cfg2 = {
        "taker_flow_gate_mode": "shadow",
        "taker_flow_contra_imb_hard": 0.2,
        "taker_flow_contra_z_hard": 2.0,
    }
    indicators = {"taker_flow_imb": -0.3, "taker_flow_imb_z": -2.5, "taker_buy_rate_ema": 100.0}
    res = eval_taker_flow_gate("LONG", indicators, cfg2)
    assert res.veto == 0
    assert res.shadow_veto == 1
    assert res.soft == 1
    assert res.reason == "contra"
