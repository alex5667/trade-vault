import numpy as np

from ml_analysis.tools.fit_confidence_bonus_weights_v1 import _auc_roc, _brier, _extract_conf_keys, _logit, _sigmoid


def test_sigmoid():
    assert np.isclose(_sigmoid(0), 0.5)
    assert _sigmoid(100) > 0.99
    assert _sigmoid(-100) < 0.01

def test_logit():
    assert np.isclose(_logit(0.5), 0.0)
    assert _logit(0.99) > 0
    assert _logit(0.01) < 0

def test_auc_roc():
    y = np.array([0, 0, 1, 1])
    p = np.array([0.1, 0.4, 0.35, 0.8])
    auc = _auc_roc(y, p)
    # Ranks:
    # 0.1 -> 1 (neg)
    # 0.35 -> 2 (pos)
    # 0.4 -> 3 (neg)
    # 0.8 -> 4 (pos)
    # Sum pos = 6. n_pos = 2. U = 6 - 3 = 3. U / (2 * 2) = 0.75
    assert np.isclose(auc, 0.75)

def test_brier():
    y = np.array([0, 1])
    p = np.array([0.2, 0.9])
    # (0.2^2 + 0.1^2) / 2 = (0.04 + 0.01) / 2 = 0.025
    assert np.isclose(_brier(y, p), 0.025)

def test_extract_conf_keys():
    ev = {"reclaim": 1.0, "obi_stable": 0}
    confs = ["sweep_eqh=1", "rsi_agree"]
    keys = _extract_conf_keys(ev, confs)
    assert "reclaim" in keys
    assert "obi_stable" in keys
    assert "sweep_eqh" in keys
    assert "rsi_agree" in keys
