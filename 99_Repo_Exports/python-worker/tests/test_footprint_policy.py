from __future__ import annotations

from core.footprint_policy import fp_confirmations_from_microbar, is_soft_confirmation


class B:
    fp_enabled = True
    fp_n_buckets = 20
    fp_max_imbalance = 0.86
    fp_absorb_score = 1.40
    fp_progress = 0.20
    fp_absorption_bias = "LONG"


def test_fp_imb_and_fp_absorb_split():
    cfg = {
        "fp_imb_min": 0.80,
        "fp_imb_min_buckets": 8,
        "fp_absorb_min_score": 1.0,
        "fp_absorb_min_imbalance": 0.65,
        "fp_absorb_max_progress": 0.35,
        "fp_absorb_require_bias_match": True,
    }
    confs = fp_confirmations_from_microbar(B(), "LONG", cfg)
    assert any(c.startswith("fp_imb=") for c in confs)
    assert any(c.startswith("fp_absorb=") for c in confs)


def test_fp_absorb_requires_bias_match():
    cfg = {
        "fp_imb_min": 0.80,
        "fp_imb_min_buckets": 8,
        "fp_absorb_min_score": 1.0,
        "fp_absorb_min_imbalance": 0.65,
        "fp_absorb_max_progress": 0.35,
        "fp_absorb_require_bias_match": True,
    }
    confs = fp_confirmations_from_microbar(B(), "SHORT", cfg)
    # imbalance still present
    assert any(c.startswith("fp_imb=") for c in confs)
    # absorption should be absent due to bias mismatch
    assert not any(c.startswith("fp_absorb=") for c in confs)


def test_fp_imb_is_soft():
    assert is_soft_confirmation("fp_imb=0.81")
    assert not is_soft_confirmation("fp_absorb=1.20")
