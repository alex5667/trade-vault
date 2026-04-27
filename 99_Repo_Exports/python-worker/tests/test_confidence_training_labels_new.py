from tools.train_confidence_calibration import _label


def test_label_target_hit():
    assert _label("target_hit", None) == 1


def test_label_stop_hit():
    assert _label("stop_hit", 10.0) == 0


def test_label_manual_exit_profit_win():
    assert _label("manual_exit", 0.1) == 1
    assert _label("manual_exit", 0.0) == 0
    assert _label("manual_exit", -0.1) == 0


def test_label_expired_no_entry_excluded():
    assert _label("expired_no_entry", None) is None


def test_label_expired_no_target_is_loss():
    assert _label("expired_no_target", None) == 0
