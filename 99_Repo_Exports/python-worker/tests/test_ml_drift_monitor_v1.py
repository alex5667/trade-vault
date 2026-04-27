from tools.ml_drift_monitor_v1 import calibration_bins, ks_stat, psi



def test_ks_stat_basic():
    a = [0.0, 0.1, 0.2, 0.3]
    b = [0.0, 0.1, 0.2, 0.3]
    assert ks_stat(a, b) == 0.0

    c = [0.9, 0.95, 1.0]
    d = ks_stat(a, c)
    assert 0.0 < d <= 1.0



def test_psi_zero_when_same():
    a = [0.1 * i for i in range(100)]
    b = list(a)
    v = psi(a, b, bins=10)
    assert abs(v) < 1e-12



def test_psi_positive_when_shifted():
    a = [0.1] * 100 + [0.9] * 100
    b = [0.2] * 100 + [0.8] * 100
    v = psi(a, b, bins=10)
    assert v >= 0.0
    assert v > 0.01



def test_calibration_bins_ece_sane():
    p = [0.1] * 50 + [0.9] * 50
    y = [0] * 50 + [1] * 50
    out = calibration_bins(p, y, n_bins=10)
    assert out["n"] == 100
    assert 0.0 <= out["ece"] <= 1.0
    assert out["ece"] < 0.05

