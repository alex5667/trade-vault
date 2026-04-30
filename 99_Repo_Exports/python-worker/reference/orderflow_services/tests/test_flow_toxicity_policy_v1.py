# -*- coding: utf-8 -*-
"""Unit tests for FlowToxicity policy decisions (Phase D / P3)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from services.orderflow.flow_toxicity import evaluate_flow_toxicity


def test_default_profile_is_annotate_only():
    dec = evaluate_flow_toxicity(
        profile="default"
        ofi_norm_z=4.0
        thr_ofi_norm_z=3.0
        vpin_cdf=0.0
        thr_vpin_cdf=0.0
        tca_is_p95_bps=0.0
        tca_perm_impact_p95_bps=0.0
        thr_is_p95_bps=0.0
        thr_perm_impact_p95_bps=0.0
        tighten_mult=1.0
        tighten_cap_bps=10.0
    )
    assert dec.hit is True
    assert dec.mode == "monitor"
    assert dec.tighten_add_bps == 0.0
    assert dec.veto is False


def test_strict_profile_tightens_but_does_not_veto():
    dec = evaluate_flow_toxicity(
        profile="strict"
        ofi_norm_z=5.0
        thr_ofi_norm_z=3.0
        vpin_cdf=0.0
        thr_vpin_cdf=0.0
        tca_is_p95_bps=0.0
        tca_perm_impact_p95_bps=0.0
        thr_is_p95_bps=0.0
        thr_perm_impact_p95_bps=0.0
        tighten_mult=2.0
        tighten_cap_bps=10.0
    )
    assert dec.hit is True
    assert dec.mode == "tighten"
    assert dec.tighten_add_bps > 0.0
    assert dec.veto is False


def test_hard_profile_veto_requires_tca_by_default():
    # toxic flow but TCA thresholds disabled => no veto
    dec1 = evaluate_flow_toxicity(
        profile="hard"
        ofi_norm_z=5.0
        thr_ofi_norm_z=3.0
        vpin_cdf=0.0
        thr_vpin_cdf=0.0
        tca_is_p95_bps=10.0
        tca_perm_impact_p95_bps=10.0
        thr_is_p95_bps=0.0
        thr_perm_impact_p95_bps=0.0
        tighten_mult=1.0
        tighten_cap_bps=10.0
        veto_without_tca=False
    )
    assert dec1.hit is True
    assert dec1.mode == "veto"
    assert dec1.veto is False

    # enable TCA threshold and make it bad => veto
    dec2 = evaluate_flow_toxicity(
        profile="hard"
        ofi_norm_z=5.0
        thr_ofi_norm_z=3.0
        vpin_cdf=0.0
        thr_vpin_cdf=0.0
        tca_is_p95_bps=12.0
        tca_perm_impact_p95_bps=0.0
        thr_is_p95_bps=5.0
        thr_perm_impact_p95_bps=0.0
        tighten_mult=1.0
        tighten_cap_bps=10.0
        veto_without_tca=False
    )
    assert dec2.hit is True
    assert dec2.veto is True


def test_vpin_path_triggers_hit_and_tighten():
    dec = evaluate_flow_toxicity(
        profile="strict"
        ofi_norm_z=0.0
        thr_ofi_norm_z=0.0
        vpin_cdf=0.99
        thr_vpin_cdf=0.95
        tca_is_p95_bps=0.0
        tca_perm_impact_p95_bps=0.0
        thr_is_p95_bps=0.0
        thr_perm_impact_p95_bps=0.0
        tighten_mult=10.0
        tighten_cap_bps=10.0
    )
    assert dec.hit is True
    assert dec.tighten_add_bps > 0.0
