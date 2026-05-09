"""Tests for util_floor_opt_v1 module."""

import numpy as np

from core.util_floor_opt_v1 import best_floor_by_sum_util


def test_best_floor_max_sum_util():
    """Test that best floor maximizes sum utility subject to min_trades constraint."""
    score = np.array([-0.02, 0.00, 0.01, 0.03, 0.06], dtype=float)
    util = np.array([-0.05, 0.00, 0.02, 0.01, 0.10], dtype=float)
    r = best_floor_by_sum_util(
        score=score,
        util_true=util,
        floor_min=-0.02,
        floor_max=0.06,
        floor_step=0.01,
        min_trades=2
    )
    assert r.n_take >= 2
    assert r.sum_util >= 0.12 - 1e-9


def test_best_floor_empty_input():
    """Test that empty input returns default result."""
    score = np.array([], dtype=float)
    util = np.array([], dtype=float)
    r = best_floor_by_sum_util(
        score=score,
        util_true=util,
        floor_min=-0.05,
        floor_max=0.10,
        floor_step=0.005,
        min_trades=1
    )
    assert r.n_take == 0
    assert r.sum_util < 0


def test_best_floor_min_trades_constraint():
    """Test that min_trades constraint is enforced."""
    score = np.array([0.01, 0.02, 0.03], dtype=float)
    util = np.array([0.05, 0.10, 0.15], dtype=float)
    r = best_floor_by_sum_util(
        score=score,
        util_true=util,
        floor_min=0.00,
        floor_max=0.10,
        floor_step=0.01,
        min_trades=5  # More than available
    )
    assert r.n_take == 0

