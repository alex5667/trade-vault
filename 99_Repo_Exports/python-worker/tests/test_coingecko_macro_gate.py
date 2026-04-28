import pytest
from core.coingecko_macro_gate import CoinGeckoMacroGate

def test_coingecko_macro_gate_no_data():
    gate = CoinGeckoMacroGate()
    # Pustye indicators
    res = gate.evaluate({}, "BUY")
    assert res.risk_off is False
    assert res.alt_weakness is False
    assert res.confidence_penalty == 0.0

def test_coingecko_macro_gate_risk_off():
    gate = CoinGeckoMacroGate(stable_dom_mom_risk_off_th=0.02)
    ind = {
        "cg_stable_dom_mom": 0.03,
        "cg_btc_dom_mom": 0.0
    }
    res = gate.evaluate(ind, "BUY")
    assert res.risk_off is True
    assert res.confidence_penalty > 0
    assert res.risk_mult < 1.0
    assert "RiskOff" in res.reason

def test_coingecko_macro_gate_alt_weakness():
    gate = CoinGeckoMacroGate()
    ind = {
        "cg_stable_dom_mom": 0.0,
        "cg_btc_dom_mom": 0.01,
        "cg_symbol_rel_strength_btc_1h": -1.5
    }
    res = gate.evaluate(ind, "BUY")
    assert res.alt_weakness is True
    assert res.confidence_penalty > 0
    assert "AltWeakness" in res.reason

def test_coingecko_macro_gate_safe():
    gate = CoinGeckoMacroGate(stable_dom_mom_risk_off_th=0.02)
    ind = {
        "cg_stable_dom_mom": 0.01, # Safe
        "cg_btc_dom_mom": 0.01,
        "cg_symbol_rel_strength_btc_1h": 1.5 # Alt outperforming
    }
    res = gate.evaluate(ind, "BUY")
    assert res.risk_off is False
    assert res.alt_weakness is False
    assert res.confidence_penalty == 0.0

def test_coingecko_macro_gate_sell_direction():
    # Macro gate risk primarily restricts BUYs, not SELLs in this logic
    gate = CoinGeckoMacroGate(stable_dom_mom_risk_off_th=0.02)
    ind = {
        "cg_stable_dom_mom": 0.05,
        "cg_btc_dom_mom": 0.01,
        "cg_symbol_rel_strength_btc_1h": -1.5
    }
    res = gate.evaluate(ind, "SELL")
    assert res.risk_off is False
    assert res.alt_weakness is False
    assert res.confidence_penalty == 0.0
