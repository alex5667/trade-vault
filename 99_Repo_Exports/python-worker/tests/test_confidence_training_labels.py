from common.confidence_training import label_outcome

def test_label_target_hit():
    assert label_outcome("target_hit", None, eps_r=0.05) == 1

def test_label_stop_hit():
    assert label_outcome("stop_hit", None, eps_r=0.05) == 0

def test_label_manual_exit_profit_win():
    assert label_outcome("manual_exit", 0.2, eps_r=0.05) == 1

def test_label_manual_exit_loss():
    assert label_outcome("manual_exit", -0.2, eps_r=0.05) == 0

def test_label_manual_exit_neutral_excluded():
    assert label_outcome("manual_exit", 0.01, eps_r=0.05) is None

def test_label_expired_no_target_uses_realized_r():
    assert label_outcome("expired_no_target", 0.2, eps_r=0.05) == 1
    assert label_outcome("expired_no_target", -0.2, eps_r=0.05) == 0
    assert label_outcome("expired_no_target", 0.0, eps_r=0.05) is None

def test_label_expired_no_entry_excluded():
    assert label_outcome("expired_no_entry", 1.0, eps_r=0.05) is None
