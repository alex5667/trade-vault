from __future__ import annotations

from types import SimpleNamespace
from core.absorption_level_score import compute_absorption_level_score, _get


def test_abs_lvl_ok_when_bias_matches_and_components_present():
    bar = SimpleNamespace(
        fp_enabled=True,
        fp_absorption_bias="LONG",
        fp_ladder_low_len=3,
        fp_ladder_high_len=0,
        fp_poc_on_edge=1,
        fp_eff_delta=0.0001,
    )
    cfg = {
        "abs_lvl_z_th": 2.0,
        "abs_lvl_eff_th": 0.0008,
        "abs_lvl_score_th": 0.60,
        "abs_lvl_ladder_norm": 3.0,
        "abs_lvl_w1": 0.30,
        "abs_lvl_w2": 0.20,
        "abs_lvl_w3": 0.20,
        "abs_lvl_w4": 0.20,
        "abs_lvl_w5": 0.10,
    }
    r = compute_absorption_level_score(
        bar=bar,
        direction="LONG",
        delta_z=2.5,
        weak_progress=True,
        iceberg_strict=True,
        reclaim_recent=True,
        cfg=cfg,
    )
    assert r.dir_match is True
    assert r.ok is True
    assert r.score >= 0.60


def test_abs_lvl_not_ok_if_bias_mismatch():
    bar = SimpleNamespace(
        fp_enabled=True,
        fp_absorption_bias="SHORT",
        fp_ladder_low_len=3,
        fp_ladder_high_len=0,
        fp_poc_on_edge=1,
        fp_eff_delta=0.0001,
    )
    cfg = {"abs_lvl_score_th": 0.0}
    r = compute_absorption_level_score(
        bar=bar,
        direction="LONG",
        delta_z=3.0,
        weak_progress=True,
        iceberg_strict=True,
        reclaim_recent=True,
        cfg=cfg,
    )
    assert r.dir_match is False
    assert r.ok is False
def test_abs_lvl_eff_quote_fallback():
    # Only legacy eff_delta present
    bar = SimpleNamespace(
        fp_enabled=True,
        fp_absorption_bias="LONG",
        fp_ladder_low_len=3,
        fp_eff_delta=0.0001,
    )
    cfg = {"abs_lvl_eff_quote_th": 0.0020, "abs_lvl_score_th": 0.1}
    r = compute_absorption_level_score(
        bar=bar, direction="LONG", delta_z=2.5, weak_progress=True,
        iceberg_strict=True, reclaim_recent=True, cfg=cfg,
    )
    assert r.ok is True
    assert r.eff_delta == 0.0001

def test_abs_lvl_notional_filter():
    bar = SimpleNamespace(
        fp_enabled=True,
        fp_absorption_bias="LONG",
        fp_ladder_low_len=3,
        fp_eff_quote=0.0001,
        fp_quote_delta=50.0,
    )
    # min_quote_delta = 100
    cfg = {"abs_lvl_min_quote_delta": 100.0, "abs_lvl_eff_quote_th": 0.0020, "abs_lvl_score_th": 0.1}
    r = compute_absorption_level_score(
        bar=bar, direction="LONG", delta_z=2.5, weak_progress=True,
        iceberg_strict=True, reclaim_recent=True, cfg=cfg,
    )
    # Global OK might be True because of other components (s2, s3, s4), 
    # but s1 must be 0.0 due to notional filter.
    assert r.parts["s1_z_wp_eff"] == 0.0
    
    # min_quote_delta = 10
    cfg["abs_lvl_min_quote_delta"] = 10.0
    r = compute_absorption_level_score(
        bar=bar, direction="LONG", delta_z=2.5, weak_progress=True,
        iceberg_strict=True, reclaim_recent=True, cfg=cfg,
    )
    assert r.parts["s1_z_wp_eff"] == 1.0 # Passed


def test_get_helper_dict_access():
    """Test _get helper with dict access (replay-friendly)."""
    bar = {
        "fp_absorption_bias": "LONG",
        "fp_ladder_low_len": 3,
        "fp_ladder_high_len": 0,
        "fp_poc_on_edge": 1,
        "fp_eff_quote": 0.0001,
        "fp_quote_delta": 50.0,
    }
    assert _get(bar, "fp_absorption_bias", "NONE") == "LONG"
    assert _get(bar, "fp_ladder_low_len", 0) == 3
    assert _get(bar, "missing", "default") == "default"


def test_abs_lvl_with_dict_bar():
    """Test compute_absorption_level_score with dict bar (replay-friendly)."""
    bar = {
        "fp_enabled": True,
        "fp_absorption_bias": "LONG",
        "fp_ladder_low_len": 3,
        "fp_ladder_high_len": 0,
        "fp_poc_on_edge": 1,
        "fp_eff_quote": 0.0001,
        "fp_quote_delta": 50.0,
    }
    cfg = {
        "abs_lvl_z_th": 2.0,
        "abs_lvl_eff_quote_th": 0.0020,
        "abs_lvl_score_th": 0.60,
        "abs_lvl_ladder_norm": 3.0,
        "abs_lvl_w1": 0.30,
        "abs_lvl_w2": 0.20,
        "abs_lvl_w3": 0.20,
        "abs_lvl_w4": 0.20,
        "abs_lvl_w5": 0.10,
    }
    r = compute_absorption_level_score(
        bar=bar,
        direction="LONG",
        delta_z=2.5,
        weak_progress=True,
        iceberg_strict=True,
        reclaim_recent=True,
        cfg=cfg,
    )
    assert r.dir_match is True
    assert r.ok is True
    assert r.score >= 0.60
