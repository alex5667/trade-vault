import math
import pytest


def _std_eff(values):
    """Mirror detector's std_eff logic: std_eff = max(std_dev, std_floor)."""
    n = len(values)
    assert n > 0
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n
    std_dev = math.sqrt(var) if var > 0 else 0.0
    mean_abs = sum(abs(v) for v in values) / n
    std_floor = max(1e-6, 0.10 * mean_abs)
    return mean, max(std_dev, std_floor)


@pytest.mark.parametrize(
    "N,x"
    [
        # Typical regimes that yield ~10–20% self-inclusion bias for |z|
        (20, 3.0),   # ~18.35%
        (30, 3.0),   # ~13.40%
        (36, 4.0),   # ~17.58%
        (50, 4.0),   # ~13.61%
        (50, 5.0),   # ~18.89%
        (60, 5.0),   # ~16.47%
        (80, 5.0),   # ~13.13%
    ]
)
def test_self_inclusion_bias_is_10_20_percent_for_typical_cases(N, x):
    """
    Quantifies the self-inclusion bias (when current sample is included into stats before z is computed).

    bias_fraction = 1 - |z_incl|/|z_prev|

    This is NOT universal for all N/x, but for typical window sizes and moderate spikes
    the bias usually lands around 10–20%. These cases are chosen as deterministic regression anchors.
    """
    prev = [1.0 if i % 2 == 0 else -1.0 for i in range(N)]  # mean=0
    mean_prev, std_prev = _std_eff(prev)
    z_prev = (x - mean_prev) / std_prev

    mean_incl, std_incl = _std_eff(prev + [x])
    z_incl = (x - mean_incl) / std_incl

    assert abs(z_incl) < abs(z_prev), "Self-inclusion must reduce |z|"
    bias_pct = 100.0 * (1.0 - (abs(z_incl) / abs(z_prev)))

    assert 10.0 <= bias_pct <= 20.0, f"bias_pct={bias_pct:.6f}% for N={N}, x={x}"
