
from core.data_health import compute_data_health


def test_data_health_hard_fail_missing_tick_ts():
    dh = compute_data_health(indicators={"tick_ts_missing": 1}, cfg={})
    assert dh.score == 0.0
    assert dh.tick_time_ok == 0


def test_data_health_degrades_on_book_unhealthy():
    dh = compute_data_health(
        indicators={"tick_ts_missing": 0, "book_health_ok": 0, "book_age_ms": 5000},
        cfg={},
    )
    assert 0.0 <= dh.score < 1.0
    assert dh.book_health_ok == 0


def test_data_health_spread_check_optional():
    dh = compute_data_health(indicators={"spread_bps": 50.0}, cfg={"data_health_spread_max_bp": 20.0})
    assert dh.spread_ok == 0

def test_apply_shadow_only_policy():
    from core.data_health import apply_shadow_only_policy
    
    # Test that score below threshold enables shadow mode
    indicators = {}
    dh = compute_data_health(indicators={"tick_ts_missing": 0, "book_health_ok": 0, "book_age_ms": 15000}, cfg={})
    # dh.score should be degraded
    cfg = {"data_health_shadow_only_below": 0.99} # Make threshold very high to guarantee failure
    apply_shadow_only_policy(indicators=indicators, dh=dh, cfg=cfg)
    assert indicators.get("data_health_shadow_only") == 1
    
    # Test that score above threshold leaves shadow mode off
    indicators2 = {}
    dh2 = compute_data_health(indicators={"tick_ts_missing": 0}, cfg={})
    cfg2 = {"data_health_shadow_only_below": 0.10} # Make threshold very low to guarantee pass
    apply_shadow_only_policy(indicators=indicators2, dh=dh2, cfg=cfg2)
    assert indicators2.get("data_health_shadow_only", 0) == 0
