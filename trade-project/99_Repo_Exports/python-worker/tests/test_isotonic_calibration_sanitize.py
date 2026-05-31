from common.isotonic_calibration import IsotonicCalibrator, fit_isotonic_pav


def test_fit_isotonic_monotone():
    # специально "ломаем" монотонность частот, PAV должен выправить
    samples = [
        (0.1, 1, 1.0),
        (0.2, 0, 1.0),
        (0.3, 1, 1.0),
        (0.4, 0, 1.0),
    ]
    cal = fit_isotonic_pav(samples).sanitize()
    assert cal.x
    assert cal.p
    for i in range(1, len(cal.p)):
        assert cal.p[i] >= cal.p[i - 1]


def test_predict_bounds():
    cal = IsotonicCalibrator(x=[0.0, 1.0], p=[0.2, 0.8], mode="linear").sanitize()
    assert 0.0 <= cal.predict(-1.0) <= 1.0
    assert 0.0 <= cal.predict(0.5) <= 1.0
    assert 0.0 <= cal.predict(10.0) <= 1.0
