from services.post_sl_analyzer import compute_sl_hit_near_liqmap_peak, is_tp1_anchored_by_liqmap

def test_sl_hit_near_liqmap_peak_explicit_price_long():
    indicators = {
        "liqmap_1h_peak_dn_price": 90.0,
        "liqmap_1h_peak_dn_usd": 300000.0,
    }
    flag, dist_bps, peak_usd = compute_sl_hit_near_liqmap_peak(
        side="LONG",
        entry_price=100.0,
        exit_price=90.01,   # 1 bp away from 90.0 (relative to entry)
        indicators=indicators,
        window="1h",
        min_usd=250000.0,
        near_bps=10.0,
    )
    assert flag == 1
    assert peak_usd >= 250000.0
    assert dist_bps <= 10.0

def test_sl_hit_near_liqmap_peak_derived_price_short():
    # No explicit peak_up_price: derive from entry and dist_up_bps.
    indicators = {
        "liqmap_1h_dist_up_bps": 1000.0,   # +10%
        "liqmap_1h_peak_up1_usd": 500000.0,
    }
    flag, dist_bps, peak_usd = compute_sl_hit_near_liqmap_peak(
        side="SHORT",
        entry_price=100.0,
        exit_price=110.01,  # ~1 bp away from derived 110.0
        indicators=indicators,
        window="1h",
        min_usd=250000.0,
        near_bps=10.0,
    )
    assert flag == 1
    assert peak_usd >= 250000.0
    assert dist_bps <= 10.0

def test_sl_hit_near_liqmap_peak_min_usd_blocks():
    indicators = {
        "liqmap_1h_dist_dn_bps": 1000.0,
        "liqmap_1h_peak_dn1_usd": 10000.0,  # too small
    }
    flag, dist_bps, peak_usd = compute_sl_hit_near_liqmap_peak(
        side="LONG",
        entry_price=100.0,
        exit_price=90.0,
        indicators=indicators,
        window="1h",
        min_usd=250000.0,
        near_bps=10.0,
    )
    assert flag == 0
    assert dist_bps == 0.0

def test_tp1_anchored_detection():
    indicators = {
        "liqmap_levels_applied": 1,
        "liqmap_tp1_adj_bps": -12.5,
        "liqmap_levels_reason": "tp1_before_peak",
        "liqmap_tp1_anchor_usd": 300000.0,
    }
    assert is_tp1_anchored_by_liqmap(indicators) == 1

if __name__ == "__main__":
    test_sl_hit_near_liqmap_peak_explicit_price_long()
    test_sl_hit_near_liqmap_peak_derived_price_short()
    test_sl_hit_near_liqmap_peak_min_usd_blocks()
    test_tp1_anchored_detection()
    print("test_post_sl_analyzer_liqmap_kpi_v1.py OK")
