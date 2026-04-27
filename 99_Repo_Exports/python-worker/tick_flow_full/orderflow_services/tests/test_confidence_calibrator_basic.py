from orderflow_services.confidence_calibrator import ConfidenceCalibrator


def test_identity_bounds():
    c = ConfidenceCalibrator(type="identity")
    assert c.apply(-1.0) == 0.0
    assert c.apply(2.0) == 1.0


def test_temp_monotonic():
    c = ConfidenceCalibrator(type="temp_logit", t=1.5)
    a = c.apply(0.2)
    b = c.apply(0.8)
    assert a < b


def test_platt_monotonic_default():
    c = ConfidenceCalibrator(type="platt_logit", a=1.0, b=0.0)
    a = c.apply(0.2)
    b = c.apply(0.8)
    assert a < b
