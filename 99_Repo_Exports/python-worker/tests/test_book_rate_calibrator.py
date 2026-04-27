import json
from core.book_rate_calibrator import BookRateCalibrator


def test_book_rate_calibrator_not_ready_uses_defaults():
    c = BookRateCalibrator(min_samples=10, dt_max_ms=2000)
    th = c.thresholds(regime="na", default_min_hz=7.0, default_warn_hz=5.0)
    assert th.src == "static"
    assert th.min_hz == 7.0
    assert th.warn_hz == 5.0


def test_book_rate_calibrator_ready_uses_p10():
    c = BookRateCalibrator(min_samples=5, dt_max_ms=2000)
    # Feed 10 samples with dt=100ms => inst ~10Hz, but vary
    for hz in [8, 9, 10, 11, 12, 9, 10, 10, 11, 10]:
        c.update(regime="range", inst_hz=float(hz), dt_ms=100)
    th = c.thresholds(regime="range", default_min_hz=5.0, default_warn_hz=3.0)
    assert th.src.startswith("calib")
    assert th.n >= 5
    assert th.min_hz > 0
    assert th.warn_hz >= th.min_hz


def test_book_rate_persistence_roundtrip():
    c = BookRateCalibrator(min_samples=5, dt_max_ms=2000)
    for hz in [8, 9, 10, 11, 12, 9, 10]:
        c.update(regime="na", inst_hz=float(hz), dt_ms=100)
    st = c.dump_regime_state(symbol="BTCUSDT", regime="na", updated_ts_ms=123)
    raw = json.dumps(st)
    c2 = BookRateCalibrator()
    c2.load_regime_state(json.loads(raw))
    th2 = c2.thresholds(regime="na", default_min_hz=5.0, default_warn_hz=3.0)
    assert th2.n == st["n"]
