from __future__ import annotations

from common.ml_labeling import (
    compute_r_mult_from_closed,
    compute_r_mult_from_pnl_risk,
    compute_y_and_r_from_closed,
    compute_y_from_r_mult,
)


def test_compute_r_mult_from_pnl_risk_ok():
    r, src = compute_r_mult_from_pnl_risk(10.0, 2.0)
    assert abs(r - 5.0) < 1e-9
    assert src == "pnl_over_risk"


def test_compute_r_mult_from_pnl_risk_no_risk():
    r, src = compute_r_mult_from_pnl_risk(10.0, 0.0)
    assert r == 0.0
    assert src == "no_risk"


def test_compute_r_mult_from_closed_prefers_field():
    fields = {"r_mult": "1.25", "pnl": "999", "risk_usd": "1"}
    r, src = compute_r_mult_from_closed(fields)
    assert abs(r - 1.25) < 1e-9
    assert src == "r_mult"


def test_compute_r_mult_from_closed_pnl_over_risk():
    fields = {"pnl": "-2.0", "risk_usd": "4"}
    r, src = compute_r_mult_from_closed(fields)
    assert abs(r + 0.5) < 1e-9
    assert src == "pnl_over_risk"


def test_compute_y_from_r_mult():
    assert compute_y_from_r_mult(0.49, 0.5) == 0
    assert compute_y_from_r_mult(0.5, 0.5) == 1


def test_compute_y_and_r_from_closed():
    y, r, src = compute_y_and_r_from_closed({"pnl": 1.0, "risk_usd": 2.0}, r_min=0.5)
    assert y == 1
    assert abs(r - 0.5) < 1e-9
    assert src == "pnl_over_risk"
