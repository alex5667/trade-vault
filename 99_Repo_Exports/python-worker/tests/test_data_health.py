
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
