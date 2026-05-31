from __future__ import annotations

from tools.auto_train_conf_calibration import _worst_topm_weighted_mean


def test_worst_topm_weighted_mean_basic():
    pairs = [(0.01, 100), (0.02, 50), (0.005, 200), (0.03, 10)]
    # top-3 deltas: 0.03(w10), 0.02(w50), 0.01(w100)
    v = _worst_topm_weighted_mean(pairs, 3)
    assert v > 0.0
    assert 0.01 <= v <= 0.03


def test_worst_topm_handles_small_n():
    pairs = [(0.02, 10)]
    v = _worst_topm_weighted_mean(pairs, 3)
    assert abs(v - 0.02) < 1e-12
