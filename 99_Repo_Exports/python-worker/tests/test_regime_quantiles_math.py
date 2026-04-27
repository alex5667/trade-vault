from __future__ import annotations

from core.regime_quantiles_store import approx_quantile_3pt


def test_quantile_piecewise():
    """Verify piecewise linear quantile approximation."""
    # At median (q50), should be ~0.50
    q = approx_quantile_3pt(0.002, 0.001, 0.002, 0.003)
    assert 0.45 <= q <= 0.55
    
    # Above q75, should be >= 0.75
    q2 = approx_quantile_3pt(0.004, 0.001, 0.002, 0.003)
    assert q2 >= 0.75


def test_quantile_edge_cases():
    """Verify edge case handling."""
    # Zero input
    assert approx_quantile_3pt(0.0, 0.001, 0.002, 0.003) == 0.0
    
    # Below q25
    q = approx_quantile_3pt(0.0005, 0.001, 0.002, 0.003)
    assert 0.0 < q < 0.25
    
    # Between q25 and q50
    q = approx_quantile_3pt(0.0015, 0.001, 0.002, 0.003)
    assert 0.25 <= q <= 0.50
