
from core.confidence_utils import confidence_pct_to_ratio, normalize_confidence_pct


def test_normalize_confidence_pct_accepts_ratio_and_pct():
    assert normalize_confidence_pct(0.0) == 0.0
    assert normalize_confidence_pct(1.0) == 100.0
    assert normalize_confidence_pct(0.5) == 50.0
    assert normalize_confidence_pct(50.0) == 50.0
    assert normalize_confidence_pct(100.0) == 100.0


def test_normalize_confidence_pct_clamps_out_of_range():
    assert normalize_confidence_pct(-10.0) == 0.0
    assert normalize_confidence_pct(999.0) == 100.0


def test_normalize_confidence_pct_nan_inf_safe():
    assert normalize_confidence_pct(float("nan")) == 0.0
    assert normalize_confidence_pct(float("inf")) == 0.0


def test_confidence_pct_to_ratio():
    assert confidence_pct_to_ratio(0.0) == 0.0
    assert confidence_pct_to_ratio(100.0) == 1.0
    assert confidence_pct_to_ratio(50.0) == 0.5
    # legacy ratio input
    assert confidence_pct_to_ratio(0.25) == 0.25
