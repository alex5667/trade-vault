"""
Pure unit tests for signal_quality.offline_job — no DB required.

Tests cover _var_cvar, compute_quality_score, and the public API.
"""

from __future__ import annotations

import pytest

from signal_quality.offline_job import (
    ALPHA,
    MIN_N,
    LOOKBACK_DAYS,
    _var_cvar,
    compute_quality_score,  # re-exported from stdlib statistics
)


# ──────────────────────────────────────────────
# _var_cvar
# ──────────────────────────────────────────────

class TestVarCvar:
    def test_empty_list_returns_zeros(self):
        assert _var_cvar([], ALPHA) == (0.0, 0.0)

    def test_single_element(self):
        var, cvar = _var_cvar([2.0], ALPHA)
        # With n=1, idx=max(0, min(0, 0-1))=0, tail=[2.0]
        assert var == 2.0
        assert cvar == pytest.approx(2.0)

    def test_all_positive(self):
        xs = [1.0, 2.0, 3.0, 4.0, 5.0]
        var, cvar = _var_cvar(xs, 0.2)
        # sorted: [1,2,3,4,5], n=5, alpha=0.2, idx=max(0,min(4,0))=0
        # var=1.0, tail=[1.0], cvar=1.0
        assert var == 1.0
        assert cvar == pytest.approx(1.0)

    def test_mixed_returns_tail_is_negative(self):
        xs = [-3.0, -2.0, -1.0, 0.5, 1.0, 2.0]
        var, cvar = _var_cvar(xs, ALPHA)
        # n=6, idx=max(0, min(5, int(0.05*6)-1))=max(0,min(5,-1))=0
        # sorted=[-3,-2,-1,0.5,1,2], var=-3.0, tail=[-3.0], cvar=-3.0
        assert var == -3.0
        assert cvar == pytest.approx(-3.0)

    def test_known_values_10_elements(self):
        xs = list(range(1, 11))  # [1..10]
        var, cvar = _var_cvar(xs, 0.1)
        # n=10, idx=max(0, min(9, 0))=0, sorted=[1..10]
        # var=1, tail=[1], cvar=1.0
        assert var == 1
        assert cvar == pytest.approx(1.0)

    def test_var_less_than_or_equal_cvar_for_left_tail(self):
        xs = [-5.0, -4.0, 1.0, 2.0, 3.0]
        var, cvar = _var_cvar(xs, 0.4)
        # tail is always at most var, so cvar <= var in left tail
        assert cvar <= var + 1e-9


# ──────────────────────────────────────────────
# compute_quality_score
# ──────────────────────────────────────────────

class TestComputeQualityScore:
    def test_insufficient_n_returns_zero(self):
        assert compute_quality_score(2.0, 0.7, -0.5, -0.8, MIN_N - 1) == 0.0

    def test_exactly_min_n_not_zero(self):
        score = compute_quality_score(1.0, 0.6, -0.5, -0.8, MIN_N)
        assert score > 0.0

    def test_perfect_signal_high_score(self):
        # expectancy=2.0 (max), win_rate=1.0, no tail risk
        score = compute_quality_score(2.0, 1.0, 0.5, 0.5, 100)
        assert score >= 95.0

    def test_negative_expectancy_low_score(self):
        score = compute_quality_score(-1.0, 0.3, -2.0, -3.0, 100)
        assert score < 20.0

    def test_severe_cvar_applies_penalty(self):
        # Same base metrics but different CVaR
        score_normal = compute_quality_score(1.0, 0.6, -0.5, -0.5, 100)
        score_damaged = compute_quality_score(1.0, 0.6, -0.5, -5.0, 100)
        assert score_damaged < score_normal

    def test_cvar_at_minus_1_no_penalty(self):
        # cvar = -1.0: boundary, penalty = 1.0 (no reduction)
        score = compute_quality_score(1.0, 0.6, -0.5, -1.0, 100)
        assert score > 0.0

    def test_result_bounded_0_100(self):
        for exp in [-5.0, 0.0, 1.0, 5.0]:
            for wr in [0.0, 0.5, 1.0, 1.5]:
                for cvar in [-10.0, -1.0, 0.0, 1.0]:
                    score = compute_quality_score(exp, wr, 0.0, cvar, 100)
                    assert 0.0 <= score <= 100.0, (
                        f"score={score} out of range for exp={exp} wr={wr} cvar={cvar}"
                    )

    def test_weights(self):
        # With exp=2.0 (max, contributes 0.6) and wr=0.0 (contributes 0.4*0=0)
        # base = 0.6*1.0 + 0.4*0.0 = 0.6 → score=60.0
        score = compute_quality_score(2.0, 0.0, 0.0, 0.0, 100)
        assert score == pytest.approx(60.0)


# ──────────────────────────────────────────────
# Constants sanity checks
# ──────────────────────────────────────────────

class TestConstants:
    def test_alpha_in_valid_range(self):
        assert 0.0 < ALPHA < 0.5

    def test_min_n_positive(self):
        assert MIN_N > 0

    def test_lookback_days_positive(self):
        assert LOOKBACK_DAYS > 0
