from services.post_sl_analyzer import PostSlAnalyzer


def test_tp1_hit_long_at_threshold():
    tp1 = 100.0
    eps_bps = 5.0
    eps_val = tp1 * (eps_bps * 1e-4)  # 0.05
    bar_h = tp1 - eps_val
    hit, eps_val_out, trigger = PostSlAnalyzer._tp1_hit_bool("LONG", bar_h=bar_h, bar_l=0.0, tp1_price=tp1, eps_bps=eps_bps)
    assert hit is True
    assert abs(eps_val_out - eps_val) < 1e-12
    assert trigger == bar_h


def test_tp1_hit_long_just_below_threshold_false():
    tp1 = 100.0
    eps_bps = 5.0
    eps_val = tp1 * (eps_bps * 1e-4)
    bar_h = (tp1 - eps_val) - 1e-9
    hit, _, _ = PostSlAnalyzer._tp1_hit_bool("LONG", bar_h=bar_h, bar_l=0.0, tp1_price=tp1, eps_bps=eps_bps)
    assert hit is False


def test_tp1_hit_short_at_threshold():
    tp1 = 200.0
    eps_bps = 10.0
    eps_val = tp1 * (eps_bps * 1e-4)  # 0.2
    bar_l = tp1 + eps_val
    hit, eps_val_out, trigger = PostSlAnalyzer._tp1_hit_bool("SHORT", bar_h=0.0, bar_l=bar_l, tp1_price=tp1, eps_bps=eps_bps)
    assert hit is True
    assert abs(eps_val_out - eps_val) < 1e-12
    assert trigger == bar_l


def test_tp1_hit_details_signed_distance_directional():
    # LONG: if trigger above TP1 => dist_bps_signed negative (overshoot)
    det = PostSlAnalyzer._tp1_hit_details("LONG", tp1_price=100.0, eps_bps=5.0, eps_val=0.05, trigger_px=100.2)
    assert det["tp1_dist_bps_signed"] < 0

    # SHORT: if trigger below TP1 => dist_bps_signed negative (overshoot)
    det2 = PostSlAnalyzer._tp1_hit_details("SHORT", tp1_price=100.0, eps_bps=5.0, eps_val=0.05, trigger_px=99.8)
    assert det2["tp1_dist_bps_signed"] < 0
