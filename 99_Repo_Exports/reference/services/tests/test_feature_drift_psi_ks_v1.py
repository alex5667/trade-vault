from __future__ import annotations

import math

from services.nightly.feature_drift_ks import ks_report
from services.nightly.feature_drift_psi import psi_report


def test_psi_stable_vs_stable_small() -> None:
    ref = [0.0, 0.1, 0.2, 0.3, 0.4] * 40
    cur = [0.0, 0.1, 0.2, 0.3, 0.4] * 40
    rep = psi_report(ref, cur)
    assert rep.psi < 0.02
    assert abs(rep.zero_rate_delta) < 1e-9


def test_ks_location_shift_detected() -> None:
    ref = [float(i) for i in range(200)]
    cur = [float(i) + 80.0 for i in range(200)]
    rep = ks_report(ref, cur)
    assert rep.ks_stat > 0.09
    assert rep.ks_pvalue < 0.05


def test_psi_sparse_zero_inflated_feature() -> None:
    ref = [0.0] * 190 + [1.0] * 10
    cur = [0.0] * 120 + [1.0] * 80
    rep = psi_report(ref, cur)
    assert rep.psi > 0.10
    assert rep.zero_rate_delta < 0.0
    assert math.isfinite(rep.clip_rate_delta)
