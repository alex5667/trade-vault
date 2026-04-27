import pytest

from core.liqmap_gate_v1 import evaluate_liqmap_gate_v1


def _base_indicators():
    # Minimal keys used by evaluate_liqmap_gate_v1
    return {
        "atr_bps_exec": 50.0,
        # adverse: for LONG it's dist_dn/peak_dn; for SHORT it's dist_up/peak_up
        "liqmap_5m_dist_dn_bps": 20.0,
        "liqmap_5m_peak_dn1_usd": 5000.0,
        # favorable
        "liqmap_5m_dist_up_bps": 200.0,
        "liqmap_5m_peak_up1_usd": 7000.0,
    }


def _base_cfg(mode: str):
    return {
        "liqmap_gate_enable": 1,
        "liqmap_gate_mode": mode,
        "liqmap_gate_window": "5m",
        "liqmap_gate_sl_atr_mult": 1.0,
        "liqmap_gate_sl_band_mult": 1.0,
        "liqmap_gate_near_band_bps": 20.0,
        "liqmap_gate_peak_min_share": 0.05,
        "liqmap_gate_min_peak_usd": 1000.0,
        "liqmap_gate_min_rr": 1.5,
    }


def test_liqmap_gate_shadow_sets_shadow_veto_only():
    ind = _base_indicators()
    cfg2 = _base_cfg("shadow")

    res = evaluate_liqmap_gate_v1(indicators=ind, direction="LONG", cfg2=cfg2)
    assert res.mode == "SHADOW"
    assert res.shadow_veto == 1
    assert res.veto == 0
    assert res.reason in ("adverse_peak_in_sl", "adverse_too_close", "rr_low")


def test_liqmap_gate_enforce_veto():
    ind = _base_indicators()
    cfg2 = _base_cfg("enforce")

    res = evaluate_liqmap_gate_v1(indicators=ind, direction="LONG", cfg2=cfg2)
    assert res.mode == "ENFORCE"
    assert res.shadow_veto == 1
    assert res.veto == 1


def test_liqmap_gate_off_no_veto():
    ind = _base_indicators()
    cfg2 = _base_cfg("shadow")
    cfg2["liqmap_gate_enable"] = 0

    res = evaluate_liqmap_gate_v1(indicators=ind, direction="LONG", cfg2=cfg2)
    assert res.mode == "OFF"
    assert res.shadow_veto == 0
    assert res.veto == 0


def test_liqmap_gate_no_adverse_no_veto():
    ind = {
        "atr_bps_exec": 50.0,
        "liqmap_5m_dist_dn_bps": 200.0,  # adverse peak is far away
        "liqmap_5m_peak_dn1_usd": 5000.0,
        "liqmap_5m_dist_up_bps": 300.0,
        "liqmap_5m_peak_up1_usd": 8000.0,
    }
    cfg2 = _base_cfg("enforce")
    cfg2["liqmap_gate_min_rr"] = 0.0  # disable rr check

    res = evaluate_liqmap_gate_v1(indicators=ind, direction="LONG", cfg2=cfg2)
    assert res.shadow_veto == 0
    assert res.veto == 0
