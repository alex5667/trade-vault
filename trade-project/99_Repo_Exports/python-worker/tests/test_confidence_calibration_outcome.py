from __future__ import annotations

from tools.train_confidence_calibration import _label


def test_outcome_target_hit_is_win():
    assert _label("target_hit", None) == 1


def test_outcome_stop_hit_is_loss():
    assert _label("stop_hit", None) == 0


def test_outcome_manual_exit_profit_is_win():
    assert _label("manual_exit", 0.1) == 1


def test_outcome_manual_exit_nonprofit_is_loss():
    assert _label("manual_exit", 0.0) == 0
    assert _label("manual_exit", -0.01) == 0


def test_outcome_expired_no_entry_is_excluded():
    assert _label("expired_no_entry", None) is None


def test_outcome_expired_no_target_is_loss():
    assert _label("expired_no_target", None) == 0


def test_outcome_breakeven_is_loss():
    assert _label("breakeven", 0.0) == 0
