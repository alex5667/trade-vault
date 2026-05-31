from core.book_rate_calibrator import BookRateCalibrator


def test_bookrate_calib_static_until_ready():
    c = BookRateCalibrator(min_samples=10)
    for _ in range(5):
        c.update(regime="trend", inst_hz=50.0)
    th = c.thresholds(regime="trend", default_ok_min_hz=5.0, default_crit_hz=2.0)
    assert th.src == "static"
    assert th.ok_min_hz == 5.0


def test_bookrate_calib_ready_after_min_samples():
    c = BookRateCalibrator(min_samples=10)
    for _ in range(20):
        c.update(regime="trend", inst_hz=100.0)
    th = c.thresholds(regime="trend", default_ok_min_hz=5.0, default_crit_hz=2.0, hi_p50_cut=80.0)
    assert th.src == "calib"
    assert th.ok_min_hz > 5.0
    assert th.crit_hz > 2.0
