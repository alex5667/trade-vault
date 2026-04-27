from __future__ import annotations

from ml_analysis.psr_dsr import sharpe_report, probabilistic_sharpe_ratio, deflated_sharpe_ratio


def test_psr_high_for_positive_clean_track() -> None:
    xs = [0.01] * 20 + [0.02] * 20 + [0.0] * 5
    rep = sharpe_report(xs, benchmark_sr=0.0, trials=4)
    assert rep.psr > 0.8
    assert 0.0 <= rep.dsr <= 1.0


def test_dsr_penalizes_many_trials() -> None:
    sr = 0.9
    low = deflated_sharpe_ratio(observed_sr=sr, n=120, trials=2)
    high = deflated_sharpe_ratio(observed_sr=sr, n=120, trials=200)
    assert high < low


def test_psr_nan_for_too_short_track() -> None:
    out = probabilistic_sharpe_ratio(observed_sr=1.0, benchmark_sr=0.0, n=1)
    assert out != out  # nan != nan


def test_sample_moments_basic() -> None:
    from ml_analysis.psr_dsr import sample_moments
    m = sample_moments([1.0, 2.0, 3.0])
    assert m["n"] == 3
    assert m["mean"] > 0
    assert m["sharpe"] > 0
