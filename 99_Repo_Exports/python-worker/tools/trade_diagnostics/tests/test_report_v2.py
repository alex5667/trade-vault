from tools.trade_diagnostics.trade_quality_report_v2 import classify_loss

def test_cost_dominates():
    b = classify_loss(
        pnl_net=-1.0, cost_bps_val=20.0, mfe_bps_val=5.0,
        close_reason="", adverse_200_bps=0.0, time_to_mfe_ms=0,
        giveback=0.0, mfe_pnl=0.0, l2_age_ms=0.0, l2_stale_now=0.0
    )
    assert b == "COST_DOMINATES"

def test_no_follow_through():
    b = classify_loss(
        pnl_net=-1.0, cost_bps_val=1.0, mfe_bps_val=0.5,
        close_reason="", adverse_200_bps=5.0, time_to_mfe_ms=0,
        giveback=0.0, mfe_pnl=0.0, l2_age_ms=0.0, l2_stale_now=0.0
    )
    assert b == "NO_FOLLOW_THROUGH"
