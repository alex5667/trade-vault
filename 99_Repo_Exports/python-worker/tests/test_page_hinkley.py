"""Plan 3 / Step 3 — Page-Hinkley detector tests."""
from __future__ import annotations

import pytest

from core.page_hinkley import (
    PageHinkley,
    detector_for_brier_increase,
    detector_for_edge_drop,
    detector_for_slippage_residual,
)


def test_no_signal_on_constant_stream():
    ph = PageHinkley(delta=0.01, threshold=2.0, min_n=10, cooldown=0)
    for _ in range(100):
        assert ph.update(0.5) is False
    assert ph.score() == pytest.approx(0.0, abs=1e-6)


def test_no_signal_before_min_n():
    ph = PageHinkley(delta=0.0, threshold=0.001, min_n=50, cooldown=0)
    for i in range(10):
        # Even a huge jump should not fire pre-warmup
        ph.update(100.0)
    assert ph.state.n == 10


def test_signal_on_upward_drift():
    """Stream switches from mean=0 to mean=1 after sample 100 → must fire."""
    ph = PageHinkley(delta=0.05, threshold=2.0, min_n=50, cooldown=0)
    fired = False
    for _ in range(100):
        ph.update(0.0)
    for i in range(100):
        if ph.update(1.0):
            fired = True
            break
    assert fired is True


def test_no_signal_on_downward_drift_one_sided():
    """One-sided (higher=worse) → downward drift should NOT fire."""
    ph = PageHinkley(delta=0.05, threshold=2.0, min_n=50, cooldown=0)
    for _ in range(100):
        ph.update(0.0)
    for _ in range(200):
        if ph.update(-1.0):
            pytest.fail("downward drift triggered one-sided detector")


def test_reset_clears_state():
    ph = PageHinkley(delta=0.01, threshold=0.5, min_n=10, cooldown=0)
    for _ in range(50):
        ph.update(1.0)
    assert ph.state.n == 50
    ph.reset()
    assert ph.state.n == 0
    assert ph.state.cumulative == 0.0


def test_cooldown_suppresses_repeat_signals():
    ph = PageHinkley(delta=0.0, threshold=0.001, min_n=5, cooldown=100)
    # Warmup
    for _ in range(10):
        ph.update(0.0)
    # First spike → signal
    first = ph.update(1000.0)
    # Continued huge values: cooldown blocks subsequent signals
    follow_signals = sum(1 for _ in range(50) if ph.update(1000.0))
    assert first is True
    assert follow_signals == 0


def test_invalid_args():
    with pytest.raises(ValueError):
        PageHinkley(delta=-1, threshold=1, min_n=1)
    with pytest.raises(ValueError):
        PageHinkley(delta=0, threshold=0, min_n=1)
    with pytest.raises(ValueError):
        PageHinkley(delta=0, threshold=1, min_n=0)


def test_score_monotonic_under_upward_drift():
    ph = PageHinkley(delta=0.0, threshold=1000.0, min_n=1, cooldown=0)
    scores: list[float] = []
    for _ in range(50):
        ph.update(1.0)
        scores.append(ph.score())
    # cumulative grows with each over-mean sample → score non-decreasing
    assert scores[-1] >= scores[0]


def test_edge_drop_detector_fires_on_negated_loss_streak():
    """Caller feeds -util_r → loss streak (util_r = -1.0) becomes +1.0."""
    ph = detector_for_edge_drop(min_n=20)
    fired = False
    for _ in range(40):
        ph.update(-(0.0))
    for _ in range(200):
        if ph.update(-(-1.0)):  # losing trades flipped positive
            fired = True
            break
    assert fired is True


def test_brier_detector_fires_on_calibration_decay():
    ph = detector_for_brier_increase(min_n=20)
    # Good calibration: Brier ~0.05
    for _ in range(40):
        ph.update(0.05)
    fired = False
    for _ in range(300):
        # Bad calibration: Brier ~0.25
        if ph.update(0.25):
            fired = True
            break
    assert fired is True


def test_slippage_detector_constructable():
    ph = detector_for_slippage_residual(min_n=50)
    assert ph.min_n == 50
    assert ph.threshold == 5.0
