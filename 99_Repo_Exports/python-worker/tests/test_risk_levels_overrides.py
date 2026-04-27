from signals.risk_levels import compute_levels


def test_compute_levels_stop_override_changes_sl_and_tp1():
    cfg = {"STOP_MODE": "ATR", "STOP_ATR_MULT": 1.0, "TP_MODE": "RR", "TP_RR": "1,2,3"}
    levels = compute_levels(100.0, 2.0, "LONG", cfg, stop_dist_override=5.0)
    assert abs(levels["sl"] - 95.0) < 1e-9
    assert abs(levels["stop_dist"] - 5.0) < 1e-9
    assert abs(levels["tp_levels"][0] - 105.0) < 1e-9


def test_compute_levels_tp1_override_rewrites_first_tp_and_rr0():
    cfg = {"STOP_MODE": "ATR", "STOP_ATR_MULT": 1.0, "TP_MODE": "RR", "TP_RR": "1,2,3"}
    # baseline stop_dist=2.0; tp1_dist_override=6.0 => rr0=3.0
    levels = compute_levels(100.0, 2.0, "LONG", cfg, tp1_dist_override=6.0)
    assert abs(levels["stop_dist"] - 2.0) < 1e-9
    assert abs(levels["tp_levels"][0] - 106.0) < 1e-9
    assert abs(levels["rr"][0] - 3.0) < 1e-9
