"""Test that exec risk penalty is non-zero when spread_bps is missing."""

import types

from core.of_confirm_engine import OFConfirmEngine


def test_exec_risk_penalty_nonzero_on_missing():
    """If spread_bps is missing, penalty should not be 0 silently."""
    engine = OFConfirmEngine()

    runtime = types.SimpleNamespace(
        last_obi_event=None,
        last_iceberg_event=None,
        last_ofi_event=None,
        last_sweep=None,
        last_reclaim=None,
        last_wp=types.SimpleNamespace(weak_any=False),
        last_fp_edge=None,
        last_bar=None,
        last_div=None,
        last_regime="na",
        dynamic_cfg={},
        pressure=types.SimpleNamespace(is_pressure_hi=lambda ts, th: False),
        book_churn_hi=0,
    )

    cfg = {
        "spread_bps_missing_default": 5.0,
        "expected_slippage_bps_missing_default": 4.0,
        "exec_risk_ref_bps": 10.0,
        "w_exec_risk": 0.18,
    }

    indicators = {
        "spread_bps": None,  # missing
        "expected_slippage_bps": None,  # missing
        "book_health_ok": 1,
        "data_health": 1.0,
    }

    ofc, dec = engine.build(
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
        tick_ts_ms=1000000,
        price=50000.0,
        delta_z=2.5,
        runtime=runtime,
        cfg=cfg,
        indicators=indicators,
    )

    assert ofc is not None
    ev = ofc.evidence

    # Check that missing flags are set
    assert ev.get("spread_bps_missing", 0) == 1 or indicators.get("spread_bps_missing", 0) == 1
    assert ev.get("expected_slippage_missing", 0) == 1 or indicators.get("expected_slippage_missing", 0) == 1

    # Check that exec_risk_norm > 0 (penalty should be applied)
    exec_risk_norm = ev.get("exec_risk_norm", 0.0)
    assert exec_risk_norm > 0.0, f"exec_risk_norm should be > 0, got {exec_risk_norm}"

    # Check that exec_risk_bps is set (spread + slippage defaults)
    exec_risk_bps = ev.get("exec_risk_bps", 0.0)
    assert exec_risk_bps > 0.0, f"exec_risk_bps should be > 0, got {exec_risk_bps}"
    assert exec_risk_bps >= 5.0 + 4.0, f"exec_risk_bps should be at least 9.0 (5+4), got {exec_risk_bps}"

