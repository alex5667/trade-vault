from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from services.orderflow.derivatives_context_gate import evaluate_derivatives_context


def test_default_profile_only_monitors():
    dec = evaluate_derivatives_context(
        profile="default",
        funding_rate_z=4.0,
        basis_bps=12.0,
        funding_extreme=1,
        basis_extreme=1,
        oi_accel=0,
        thr_funding_z=3.0,
        thr_basis_bps=10.0,
        require_oi_for_veto=True,
        tighten_mult=1.0,
        tighten_cap_bps=8.0,
    )
    assert dec.hit is True
    assert dec.mode == "monitor"
    assert dec.tighten_add_bps == 0.0
    assert dec.veto is False


def test_strict_profile_tightens():
    dec = evaluate_derivatives_context(
        profile="strict",
        funding_rate_z=4.0,
        basis_bps=12.0,
        funding_extreme=1,
        basis_extreme=1,
        oi_accel=0,
        thr_funding_z=3.0,
        thr_basis_bps=10.0,
        require_oi_for_veto=True,
        tighten_mult=2.0,
        tighten_cap_bps=8.0,
    )
    assert dec.hit is True
    assert dec.tighten_add_bps > 0.0
    assert dec.veto is False


def test_hard_profile_requires_oi_for_veto_when_enabled():
    dec = evaluate_derivatives_context(
        profile="hard",
        funding_rate_z=4.0,
        basis_bps=12.0,
        funding_extreme=1,
        basis_extreme=1,
        oi_accel=0,
        thr_funding_z=3.0,
        thr_basis_bps=10.0,
        require_oi_for_veto=True,
        tighten_mult=1.0,
        tighten_cap_bps=8.0,
    )
    assert dec.veto is False

    dec2 = evaluate_derivatives_context(
        profile="hard",
        funding_rate_z=4.0,
        basis_bps=12.0,
        funding_extreme=1,
        basis_extreme=1,
        oi_accel=1,
        thr_funding_z=3.0,
        thr_basis_bps=10.0,
        require_oi_for_veto=True,
        tighten_mult=1.0,
        tighten_cap_bps=8.0,
    )
    assert dec2.veto is True
