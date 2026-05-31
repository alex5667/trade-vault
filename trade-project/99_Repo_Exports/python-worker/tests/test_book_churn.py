from __future__ import annotations

from core.book_churn import compute_churn_from_z


def test_churn_score_monotonic():
    a = compute_churn_from_z(rate_hz=10.0, rate_z=1.0, z_start=2.0, z_full=5.0, z_hi=4.0)
    b = compute_churn_from_z(rate_hz=10.0, rate_z=3.0, z_start=2.0, z_full=5.0, z_hi=4.0)
    c = compute_churn_from_z(rate_hz=10.0, rate_z=6.0, z_start=2.0, z_full=5.0, z_hi=4.0)
    assert a.churn_score == 0.0
    assert b.churn_score > 0.0
    assert c.churn_score == 1.0
    assert c.churn_hi == 1
