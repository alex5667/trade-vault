import pytest

from handlers.crypto_orderflow.core.crypto_orderflow_calibration import (
    ConfidenceCalibratorCfg,
    RollingPercentileCalibrator,
)


def test_fallback_mapping_is_in_0_100_and_increases_with_abs_score():
    cal = RollingPercentileCalibrator(ConfidenceCalibratorCfg(window=100, min_history=30, fallback_k=1.25))
    a = cal.calibrate(symbol="BTCUSDT", kind="breakout", final_score=0.1, update=False)
    b = cal.calibrate(symbol="BTCUSDT", kind="breakout", final_score=1.0, update=False)
    c = cal.calibrate(symbol="BTCUSDT", kind="breakout", final_score=5.0, update=False)
    assert 0.0 <= a <= 100.0
    assert 0.0 <= b <= 100.0
    assert 0.0 <= c <= 100.0
    assert a < b < c


def test_percentile_calibration_uses_history_when_enough_samples():
    cal = RollingPercentileCalibrator(ConfidenceCalibratorCfg(window=1000, min_history=5, fallback_k=1.25))
    # build history: abs(final_score)=1..10
    for i in range(1, 11):
        cal.update(symbol="ETHUSDT", kind="absorption", final_score=float(i))
    # abs=1 => around 10%
    p1 = cal.calibrate(symbol="ETHUSDT", kind="absorption", final_score=1.0, update=False)
    # abs=10 => 100%
    p10 = cal.calibrate(symbol="ETHUSDT", kind="absorption", final_score=10.0, update=False)
    assert p1 <= p10
    assert p10 == pytest.approx(100.0, abs=1e-6)


def test_nan_inf_do_not_break_calibrator():
    cal = RollingPercentileCalibrator(ConfidenceCalibratorCfg(window=100, min_history=5, fallback_k=1.25))
    p_nan = cal.calibrate(symbol="BTCUSDT", kind="breakout", final_score=float("nan"), update=False)
    p_inf = cal.calibrate(symbol="BTCUSDT", kind="breakout", final_score=float("inf"), update=False)
    assert 0.0 <= p_nan <= 100.0
    assert 0.0 <= p_inf <= 100.0
