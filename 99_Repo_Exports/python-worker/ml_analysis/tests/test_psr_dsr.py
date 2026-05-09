from __future__ import annotations

import math

from ml_analysis.psr_dsr import (
    deflated_sharpe_ratio,
    expected_max_sharpe_benchmark,
    kurtosis,
    mean,
    probabilistic_sharpe_ratio,
    sample_std,
    sharpe_ratio,
    skewness,
)


def _sin_series(n: int, mu: float, amplitude: float = 0.01) -> list:
    """Reproducible deterministic series centred on mu."""
    return [mu + amplitude * math.sin(i * 0.41) for i in range(n)]


def test_mean_std_basic():
    xs = [1.0, 2.0, 3.0]
    assert abs(mean(xs) - 2.0) < 1e-9
    assert sample_std(xs) > 0.0


def test_sample_std_constants():
    assert sample_std([5.0, 5.0, 5.0]) == 0.0


def test_skewness_symmetric():
    xs = _sin_series(200, 0.0)
    # near-symmetric series, skewness close to 0
    assert abs(skewness(xs)) < 0.5


def test_kurtosis_returns_positive():
    xs = _sin_series(100, 0.02, 0.005)
    assert kurtosis(xs) >= 1.0


def test_sharpe_positive():
    xs = _sin_series(300, 0.005, 0.002)
    sr = sharpe_ratio(xs)
    assert sr > 0.0


def test_sharpe_negative():
    xs = _sin_series(300, -0.005, 0.002)
    sr = sharpe_ratio(xs)
    assert sr < 0.0


def test_psr_good_strategy():
    xs = _sin_series(300, 0.01, 0.003)
    p = probabilistic_sharpe_ratio(xs)
    assert 0.5 < p <= 1.0


def test_psr_edge_short():
    assert probabilistic_sharpe_ratio([0.01]) == 0.0


def test_psr_flat_returns_zero_std():
    # All returns identical → std = 0, should not crash
    p = probabilistic_sharpe_ratio([0.0] * 50)
    assert 0.0 <= p <= 1.0


def test_expected_max_sharpe():
    val = expected_max_sharpe_benchmark(100, sr_std=1.0)
    assert val > 0.0
    val_1 = expected_max_sharpe_benchmark(1, sr_std=1.0)
    assert val_1 == 0.0


def test_dsr_more_trials_lower():
    xs = _sin_series(500, 0.02, 0.005)
    dsr_10 = deflated_sharpe_ratio(xs, n_trials=10)
    dsr_1000 = deflated_sharpe_ratio(xs, n_trials=1000)
    assert dsr_10 >= dsr_1000


def test_dsr_edge_empty():
    assert deflated_sharpe_ratio([]) == 0.0
