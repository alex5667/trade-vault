from __future__ import annotations

from types import SimpleNamespace
from core.of_confirm_engine import OFConfirmEngine
from utils.time_utils import get_ny_time_millis

import os

def test_strong_gate_uses_computed_flags():
    eng = OFConfirmEngine(version=2)
    now_ms = get_ny_time_millis()
    
    indicators = {
        "delta_z": 3.0,
    }
    cfg = {
        "require_strong_confirmation": True,
        "strong_gate_shadow": False,
        "obi_stable_min_secs": 1.0,
        "strong_z_min": 2.0,
        "strong_need_reversal": 2,
        "of_score_min": 0.0,
    }

    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=True),
        last_obi_event={
            "ts_ms": now_ms - 500,
            "stable_secs": 2.0,
            "obi_z": 1.5,
            "direction": "SHORT",  # Reversal SHORT delta against SHORT sweep implies sweep direction_bias matches delta direction. Wait, Reversal means we are trading AGAINST trend, or sweep is the trend context. 
            # In eval_reversal, it checks A + B + C.
        },
        last_iceberg_event=None,
        last_sweep=SimpleNamespace(
            ts_ms=now_ms - 1000,
            kind="EQH_SWEEP",
            pool_id="p1",
            level=100.0,
            tol_px=0.0,
            breach_px=100.1,
            confirm_px=99.9,
            direction_bias="SHORT",
            touches=5
        ),
        last_reclaim=SimpleNamespace(
            ts_ms=now_ms - 500,
            hold_bars=2,
            direction_bias="SHORT",
            level=99.0,
            pool_id="p1"
        ),
        last_div=None,
        cont_ctx_ts_ms=0,
    )

    ofc, dec = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="SHORT",
        tick_ts_ms=now_ms,
        price=100.0,
        delta_z=3.0,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )

    assert ofc is not None
    assert ofc.scenario == "reversal"
    assert ofc.ok == 1
    assert ofc.have >= 2
    # indicators should have obi analytical data
    assert indicators.get("obi_z") == 1.5

def test_eval_continuation_intensity_fallback():
    """Verify that intensity fallback bypasses trend_dir requirement if delta_z is strong."""
    eng = OFConfirmEngine(version=2)
    now_ms = get_ny_time_millis()

    indicators = {
        "delta_z": 4.0,  # strong delta_z >= 3.0
        "trend_dir_source": "direction"
    }
    cfg = {
        "require_strong_confirmation": True,
        "strong_gate_shadow": False,
        "strong_cont_delta_z_thr": 3.0,
        "strong_need_continuation": 2,
        "of_score_min": 0.0,
    }

    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=False),
        last_obi_event={
            "ts_ms": now_ms - 500,
            "stable_secs": 2.0,
            "direction": "LONG",
        },
        last_iceberg_event=None,
        last_sweep=None,
        last_reclaim=None,
        last_div=None,  # No hidden divergence -> no hidden_ctx_recent by default
        cont_ctx_ts_ms=now_ms - 1000,  # Provides component C
        last_regime="na",
    )

    # With trend_dir_source="direction", direction="LONG", and strong delta_z, fallback_a should be True
    # C is True (cont_ctx_recent)
    # B is True (obi_stable)
    # Total have = 3, need = 2 -> ok = 1

    os.environ["OF_TREND_DIR_FALLBACK_TO_DIRECTION"] = "1"
    try:
        ofc, dec = eng.build(
            symbol="BTCUSDT",
            tf="1s",
            direction="LONG",
            tick_ts_ms=now_ms,
            price=100.0,
            delta_z=4.0,
            runtime=runtime,
            cfg=cfg,
            indicators=indicators
        )
    finally:
        del os.environ["OF_TREND_DIR_FALLBACK_TO_DIRECTION"]

    assert ofc is not None
    assert ofc.scenario == "continuation"
    assert ofc.ok == 1

def test_abs_lvl_counts_as():
    """Verify that absorption level counts as A or C based on configuration."""
    eng = OFConfirmEngine(version=2)
    now_ms = get_ny_time_millis()

    indicators = {"delta_z": 1.0}  # Weak delta_z, so A component normally False
    cfg = {
        "require_strong_confirmation": True,
        "strong_z_min": 2.0,
        "strong_need_reversal": 2,
        "abs_lvl_enable": 1,
        "abs_lvl_counts_as": "A",
        "abs_lvl_score_th": 0.4,
        "of_score_min": 0.0,
    }

    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_wp=SimpleNamespace(weak_any=True),
        last_obi_event=None, # B is False
        last_iceberg_event=None,
        last_sweep=SimpleNamespace(
            ts_ms=now_ms - 1000,
            kind="EQH_SWEEP",
            direction_bias="LONG",
        ),
        last_reclaim=SimpleNamespace(
            ts_ms=now_ms - 500,
            direction_bias="LONG",
        ), # C is True (sweep+reclaim) Wait, sweep+reclaim is B component in eval_reversal!
        # Actually in eval_reversal: A=delta_z+wp, B=sweep+reclaim, C=obi/iceberg/etc.
        last_div=None,
        cont_ctx_ts_ms=0,
        last_bar=SimpleNamespace(
            fp_enabled=True,
            fp_absorption_bias="LONG",
            fp_ladder_low_len=5,
            fp_ladder_high_len=0,
            fp_eff_delta=-100.0,
            fp_poc_on_edge=True
        )
    )

    # A=False (delta_z < 2.0), B=True (sweep+reclaim), C=False (no obi/iceberg)
    # abs_lvl_ok = True -> counts as A
    # Total have: A(from abs)+B = 2 -> ok

    ofc, dec = eng.build(
        symbol="BTCUSDT",
        tf="1s",
        direction="LONG",
        tick_ts_ms=now_ms,
        price=100.0,
        delta_z=1.0,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators
    )

    assert ofc is not None
    assert ofc.scenario == "reversal"
    assert ofc.ok == 1

