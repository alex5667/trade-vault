from __future__ import annotations


def dist_bp(px: float, level: float) -> float:
    mid = 0.5 * (abs(px) + abs(level))
    return (10000.0 * abs(px - level) / mid) if mid > 0 else 0.0


def test_zone_dist_bp_and_near_ok_logic():
    close_px = 100.0
    close_cross_level = 99.9  # 10bp-ish
    d = dist_bp(close_px, close_cross_level)
    assert d > 0
    near_bp = 15.0
    ok_bp = 15.0
    close_cross = 1
    near_zone = 1 if (d <= near_bp) else 0
    zone_ok = 1 if (near_zone == 1 and close_cross == 1 and d <= ok_bp) else 0
    assert near_zone == 1
    assert zone_ok == 1

def test_zone_logic_fail_cases():
    # Case 1: Too far
    close_px = 100.0
    close_cross_level = 90.0 # ~1000bp
    d = dist_bp(close_px, close_cross_level)
    near_bp = 15.0
    ok_bp = 15.0
    close_cross = 1
    near_zone = 1 if (d <= near_bp) else 0
    zone_ok = 1 if (near_zone == 1 and close_cross == 1 and d <= ok_bp) else 0
    assert near_zone == 0
    assert zone_ok == 0

    # Case 2: Near but no structure context (close_cross = 0)
    close_px = 100.0
    close_cross_level = 99.95
    d = dist_bp(close_px, close_cross_level)
    close_cross = 0
    near_zone = 1 if (d <= near_bp) else 0
    zone_ok = 1 if (near_zone == 1 and close_cross == 1 and d <= ok_bp) else 0
    assert near_zone == 1
    assert zone_ok == 0
