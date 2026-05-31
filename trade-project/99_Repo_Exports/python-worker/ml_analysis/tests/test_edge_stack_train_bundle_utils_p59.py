
from ml_analysis.tools.edge_stack_train_bundle_utils_p59 import validate_dataset_report, validate_train_report


def test_validate_dataset_report_ok():
    r = {"joined": 250, "pos_rate": 0.10}
    v = validate_dataset_report(r, min_joined=200, pos_rate_min=0.05, pos_rate_max=0.60)
    assert v.ok is True
    assert v.reason == "ok"


def test_validate_dataset_report_small():
    r = {"joined": 10, "pos_rate": 0.10}
    v = validate_dataset_report(r, min_joined=200, pos_rate_min=0.05, pos_rate_max=0.60)
    assert v.ok is False
    assert "dataset_too_small" in v.reason


def test_validate_dataset_report_pos_rate_out_of_range():
    r = {"joined": 300, "pos_rate": 0.90}
    v = validate_dataset_report(r, min_joined=200, pos_rate_min=0.05, pos_rate_max=0.60)
    assert v.ok is False
    assert "pos_rate_out_of_range" in v.reason


def test_validate_train_report_ok():
    r = {"oof": {"meta": {"brier": 0.12, "ece": 0.03}}}
    v = validate_train_report(r, brier_max=0.30, ece_max=0.08)
    assert v.ok is True
    assert v.reason == "ok"


def test_validate_train_report_brier_fail():
    r = {"oof": {"meta": {"brier": 0.50, "ece": 0.03}}}
    v = validate_train_report(r, brier_max=0.30, ece_max=0.08)
    assert v.ok is False
    assert "brier_too_high" in v.reason


def test_validate_train_report_ece_fail():
    r = {"oof": {"meta": {"brier": 0.12, "ece": 0.20}}}
    v = validate_train_report(r, brier_max=0.30, ece_max=0.08)
    assert v.ok is False
    assert "ece_too_high" in v.reason


def test_validate_train_report_missing_oof_meta():
    # Empty report should fail with missing_oof_meta
    v = validate_train_report({}, brier_max=0.30, ece_max=0.08)
    assert v.ok is False
    assert "missing_oof_meta" in v.reason

