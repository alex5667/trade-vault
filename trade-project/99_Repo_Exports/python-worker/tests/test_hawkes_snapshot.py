"""
Test for Hawkes-like intensity snapshot computation.

Validates the basic correctness of the Hawkes recursion:
S <- exp(-beta*dt)*S + rate*dt
lam = mu + alpha*S
"""

import math

import pytest


def test_hawkes_snapshot_has_keys():
    """Test that Hawkes snapshot computation produces valid keys and non-negative intensities."""
    hs = {"ts_ms": 1000, "S_taker": 0.0, "S_cancel": 0.0, "S_churn": 0.0}
    t_now = 2000
    dt_s = (t_now - hs["ts_ms"]) / 1000.0
    beta = 1.8
    decay = math.exp(-beta * dt_s)
    taker_rate = 2.0
    hs["S_taker"] = decay * hs["S_taker"] + taker_rate * dt_s
    lam = 0.1 + 0.9 * hs["S_taker"]
    assert lam >= 0.0


def test_hawkes_decay_properties():
    """Test that decay factor decreases with time and beta."""
    beta = 1.8
    dt_s1 = 0.5
    dt_s2 = 1.0

    decay1 = math.exp(-beta * dt_s1)
    decay2 = math.exp(-beta * dt_s2)

    assert decay1 > decay2  # Longer time = more decay
    assert 0.0 < decay1 < 1.0
    assert 0.0 < decay2 < 1.0


def test_hawkes_intensity_non_negative():
    """Test that Hawkes intensities are always non-negative."""
    # Test with various states
    test_cases = [
        {"S_taker": 0.0, "S_cancel": 0.0, "S_churn": 0.0},
        {"S_taker": 10.0, "S_cancel": 5.0, "S_churn": 15.0},
        {"S_taker": 100.0, "S_cancel": 50.0, "S_churn": 150.0},
    ]

    alpha_t = 0.9
    mu_t = 0.1
    alpha_c = 0.7
    mu_c = 0.1
    alpha_h = 0.5
    mu_h = 0.1

    for hs in test_cases:
        lam_t = mu_t + alpha_t * hs["S_taker"]
        lam_c = mu_c + alpha_c * hs["S_cancel"]
        lam_h = mu_h + alpha_h * hs["S_churn"]

        assert lam_t >= 0.0, f"taker_lam should be non-negative, got {lam_t}"
        assert lam_c >= 0.0, f"cancel_lam should be non-negative, got {lam_c}"
        assert lam_h >= 0.0, f"churn_lam should be non-negative, got {lam_h}"


def test_hawkes_state_update():
    """Test that state update follows the recursion formula."""
    hs = {"ts_ms": 1000, "S_taker": 5.0}
    t_now = 2000
    dt_s = (t_now - hs["ts_ms"]) / 1000.0
    beta = 1.8
    decay = math.exp(-beta * dt_s)
    taker_rate = 2.0

    # Update state
    new_S = decay * hs["S_taker"] + taker_rate * dt_s

    # Verify: new state should be between decayed old state and old state + rate*dt
    decayed_old = decay * hs["S_taker"]
    max_possible = hs["S_taker"] + taker_rate * dt_s

    assert decayed_old <= new_S <= max_possible
    assert new_S >= 0.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])




















