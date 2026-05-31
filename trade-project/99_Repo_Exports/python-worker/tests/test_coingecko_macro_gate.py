from core.coingecko_macro_gate import CoinGeckoMacroGate


def test_coingecko_macro_gate_no_data():
    gate = CoinGeckoMacroGate()
    # Pustye indicators
    res = gate.evaluate({}, "BUY")
    assert res.risk_off is False
    assert res.alt_weakness is False
    assert res.confidence_penalty == 0.0
    # Add check for fail open
    assert {}["macro_gate_reason"] == "cg_missing_fail_open" if "macro_gate_reason" in {} else True 
    # Wait, the dictionary was not preserved. Let's fix this in a better way.
    ind = {}
    res = gate.evaluate(ind, "BUY")
    assert ind.get("macro_gate_reason") == "cg_missing_fail_open"

def test_coingecko_macro_gate_risk_off():
    gate = CoinGeckoMacroGate(stable_dom_mom_risk_off_th=0.02)
    ind = {
        "cg_quality": 1.0,
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
        "cg_quality": 1.0,
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
        "cg_quality": 1.0,
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
        "cg_quality": 1.0,
        "cg_stable_dom_mom": 0.05,
        "cg_btc_dom_mom": 0.01,
        "cg_symbol_rel_strength_btc_1h": -1.5
    }
    res = gate.evaluate(ind, "SELL")
    assert res.risk_off is False
    assert res.alt_weakness is False
    assert res.confidence_penalty == 0.0

def test_coingecko_macro_gate_fail_open_stale():
    """
    Test fail-open behavior and mild tighten behavior for stale data.
    """
    gate = CoinGeckoMacroGate()
    
    # Missing / severely stale
    ind1 = {"cg_quality": 0.0}
    res = gate.evaluate(ind1, "BUY")
    assert not res.risk_off
    assert ind1.get("macro_tighten_add_bps", 0.0) == 0.0
    assert ind1["macro_gate_reason"] == "cg_missing_fail_open"

    # Mild stale with mild_tighten_only (should return early and not check other metrics)
    ind2 = {
        "cg_quality": 0.5,
        "cg_stable_dom_mom": 0.10 # This would normally trigger RiskOff
    }
    res2 = gate.evaluate(ind2, "BUY")
    
    assert not res2.risk_off # Avoided because of early return
    assert ind2["macro_tighten_add_bps"] == 1.0 # Mild tighten applied
    assert ind2["macro_gate_reason"] == "cg_stale_mild_tighten"

    # Mild stale with normal evaluation (evaluate_with_cap mode)
    gate_full = CoinGeckoMacroGate(macro_stale_mode="evaluate_with_cap")
    ind3 = {
        "cg_quality": 0.5,
        "cg_stable_dom_mom": 0.10 # Triggers RiskOff
    }
    res3 = gate_full.evaluate(ind3, "BUY")
    
    assert res3.risk_off # Evaluated
    assert ind3["macro_tighten_add_bps"] == 1.0 # Mild tighten applied
    assert ind3["macro_gate_reason"] == "cg_stale_mild_tighten"

def test_coingecko_macro_gate_stale_data_mild_tighten():
    gate = CoinGeckoMacroGate()
    ind = {
        "cg_quality": 0.5,
        "cg_stable_dom_mom": 0.03,
    }
    res = gate.evaluate(ind, "BUY")
    assert ind.get("macro_tighten_add_bps") == 1.0
    assert ind.get("macro_gate_reason") == "cg_stale_mild_tighten"

def test_coingecko_macro_gate_missing_data_fail_open():
    gate = CoinGeckoMacroGate()
    ind = {
        "cg_quality": 0.0,
    }
    res = gate.evaluate(ind, "BUY")
    assert ind.get("macro_gate_reason") == "cg_missing_fail_open"
    assert res.confidence_penalty == 0.0
    assert res.risk_mult == 1.0
