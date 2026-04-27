from core.book_health import compute_book_health


def test_book_health_ok_rate_and_age():
    bh = compute_book_health(
        now_ts_ms=10_000,
        last_book_ts_ms=9_980,
        rate_hz=50.0,
        ok_min_hz=20.0,
        crit_hz=10.0,
        age_floor_ms=100,
        age_mult=3.0,
    )
    assert bh.ok == 1
    assert bh.state == "OK"


def test_book_health_err_no_ts():
    bh = compute_book_health(
        now_ts_ms=10_000,
        last_book_ts_ms=0,
        rate_hz=100.0,
        ok_min_hz=20.0,
        crit_hz=10.0,
    )
    assert bh.ok == 0
    assert bh.state == "ERR"


def test_book_health_warn_age_too_high():
    # ok_min_hz=50 => exp_dt=20ms, max_age approx 60ms
    bh = compute_book_health(
        now_ts_ms=10_000,
        last_book_ts_ms=9_800,
        rate_hz=80.0,
        ok_min_hz=50.0,
        crit_hz=10.0,
        age_floor_ms=50,
        age_mult=3.0,
    )
    assert bh.ok == 0
    assert bh.state in ("WARN", "ERR")
