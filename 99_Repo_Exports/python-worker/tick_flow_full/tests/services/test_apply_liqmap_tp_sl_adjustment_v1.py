# tick_flow_full/tests/services/test_apply_liqmap_tp_sl_adjustment_v1.py
"""Unit tests: apply_liqmap_tp_sl_adjustment (TP/SL overlay).

This test focuses on the *risk cap* behavior for SL widening.
"""


def test_apply_liqmap_tp_sl_adjustment_caps_sl_widen():
    from services.orderflow.liqmap_features import apply_liqmap_tp_sl_adjustment

    entry = 100.0
    side = "LONG"
    base_sl = 99.9  # 10 bps risk
    base_tp1 = 101.0

    # A strong downside peak between base_sl and entry.
    indicators = {
        "liqmap_1h_peak_dn_price": 99.0,
        "liqmap_1h_peak_dn1_usd": 500_000.0,
        "liqmap_1h_peak_up_price": 101.5,
        "liqmap_1h_peak_up1_usd": 100_000.0,
    }

    new_sl, new_tp1, out = apply_liqmap_tp_sl_adjustment(
        entry=entry,
        side=side,
        base_sl=base_sl,
        base_tp1=base_tp1,
        indicators=indicators,
        window="1h",
        min_usd=250_000.0,
        buffer_bps=5.0,
        max_sl_widen_bps=20.0,
        enable_tp1=False,
        enable_sl=True,
    )

    # base_stop_bps = 10; cap => total <= 30 bps => sl >= 99.7
    assert abs(new_sl - 99.7) < 1e-9, f"Expected capped SL to 99.7, got {new_sl}"
    assert new_tp1 == base_tp1
    assert int(out.get("liqmap_levels_applied", 0)) == 1
    assert out.get("liqmap_levels_reason") == "cap_sl_widen"
    assert abs(float(out.get("liqmap_sl_adj_bps", 0.0)) - 20.0) < 1e-9
