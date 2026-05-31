from __future__ import annotations

from ml_analysis.pbo_cscv import compute_pbo_cscv
from ml_analysis.reality_check import net_expectancy, precision_at_top_x, mean_r, downside_adjusted_return, hit_rate_conditioned_on_cost


def test_pbo_low_when_single_variant_is_consistently_best() -> None:
    scores = {
        "a": [0.9, 0.8, 1.0, 0.95],
        "b": [0.2, 0.1, 0.3, 0.25],
        "c": [-0.1, -0.2, 0.0, -0.1],
    }
    rep = compute_pbo_cscv(scores)
    assert rep.pbo < 0.5
    assert rep.n_splits > 0


def test_universal_evaluators_smoke() -> None:
    assert abs(net_expectancy([1.0, -0.5, 0.5]) - 1.0 / 3.0) < 1e-9
    assert abs(mean_r([1.0, -0.5, 0.5]) - 1.0 / 3.0) < 1e-9
    assert precision_at_top_x([1, 0, 1, 1], [0.9, 0.1, 0.8, 0.2], x_frac=0.5) == 1.0
    assert downside_adjusted_return([1.0, -0.5, 0.5]) > 0.0
    assert abs(hit_rate_conditioned_on_cost([5.0, 2.0, -1.0], [1.0, 1.5, 0.5]) - 2.0 / 3.0) < 1e-9


def test_pbo_cscv_result_fields() -> None:
    scores = {
        "x": [1.0, 0.9, 1.1, 0.8],
        "y": [-0.1, -0.2, 0.0, -0.3],
    }
    r = compute_pbo_cscv(scores)
    assert r.n_variants == 2
    assert r.n_periods == 4
    assert 0.0 <= r.pbo <= 1.0
