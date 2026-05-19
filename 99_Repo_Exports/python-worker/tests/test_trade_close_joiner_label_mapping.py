"""Regression: trade_close_joiner.label_to_calib_result must NOT map y=None
to "BE" — that inflates p_edge_threshold_calibrator's BE-bucket and trips
PEdgeCalibBEBucketInflated (alert ML-2).

Pre-fix: y=None → "BE" → calibrator counts it as a real breakeven trade.
Post-fix: y=None → "UNKNOWN" → calibrator drops it via result_invalid.
"""
from __future__ import annotations

from services.orderflow.trade_close_joiner_worker_v1 import label_to_calib_result


def test_y_one_is_win():
    assert label_to_calib_result(1) == "WIN"


def test_y_zero_is_loss():
    assert label_to_calib_result(0) == "LOSS"


def test_y_none_is_unknown_not_be():
    # Critical: this is the regression. Must NOT be "BE".
    assert label_to_calib_result(None) == "UNKNOWN"


def test_unknown_is_outside_calibrator_whitelist():
    # The calibrator (orderflow_services/p_edge_threshold_calibrator_v1.py:345)
    # only accepts WIN/LOSS/BE. UNKNOWN must be dropped, not inflate any bucket.
    assert label_to_calib_result(None) not in ("WIN", "LOSS", "BE")
