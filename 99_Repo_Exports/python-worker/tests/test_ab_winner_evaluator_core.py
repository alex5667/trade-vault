from __future__ import annotations

from services.ab_winner_evaluator_core import (
    choose_winner_lcb,
    compute_arm_stats,
    regime_bucket,
)


def test_regime_bucket() -> None:
    assert regime_bucket("trend") == "trend"
    assert regime_bucket("trending_bull") == "trend"
    assert regime_bucket("range") == "range"
    assert regime_bucket("thin") == "thin"
    assert regime_bucket("news") == "thin"
    assert regime_bucket("mixed") == "mixed"
    assert regime_bucket("na") == "mixed"


def test_compute_arm_stats_basic() -> None:
    arm_to_r = {
        "A": [0.1, 0.0, -0.1, 0.2, 0.0],
        "B": [0.2, 0.3, 0.1, 0.2, 0.2],
        "C": [],
    }
    st = compute_arm_stats(arm_to_r=arm_to_r, alpha=0.10)
    assert st["A"].n == 5
    assert st["B"].n == 5
    assert st["C"].n == 0
    assert st["B"].mean > st["A"].mean


def test_choose_winner_lcb_picks_b_when_strong() -> None:
    # B has meaningfully higher mean and enough n => should win by LCB
    arm_to_r = {
        "A": [0.02] * 60,
        "B": [0.20] * 60,
        "C": [0.05] * 60,
    }
    dec = choose_winner_lcb(
        regime="range",
        arm_to_r=arm_to_r,
        min_n=40,
        min_edge_by_bucket={"trend": 0.05, "range": 0.08, "mixed": 0.08, "thin": 0.12},
        alpha_by_bucket={"trend": 0.10, "range": 0.10, "mixed": 0.10, "thin": 0.05},
    )
    assert dec.winner == "B"


def test_choose_winner_lcb_thin_requires_more_edge() -> None:
    # In thin bucket min_edge is higher; even if B > A, might be rejected.
    # Construct B with small advantage.
    arm_to_r = {
        "A": [0.04] * 60,
        "B": [0.10] * 60,  # LCB close to mean because std=0, but still need >= min_edge(thin)=0.12
        "C": [],
    }
    dec = choose_winner_lcb(
        regime="thin",
        arm_to_r=arm_to_r,
        min_n=40,
        min_edge_by_bucket={"trend": 0.05, "range": 0.08, "mixed": 0.08, "thin": 0.12},
        alpha_by_bucket={"trend": 0.10, "range": 0.10, "mixed": 0.10, "thin": 0.05},
    )
    assert dec.winner == "A"
    assert "min_edge" in dec.reason or "baseline" in dec.reason


def test_choose_winner_lcb_insufficient_samples_keeps_a() -> None:
    arm_to_r = {
        "A": [0.05] * 10,
        "B": [0.50] * 10,
        "C": [],
    }
    dec = choose_winner_lcb(
        regime="range",
        arm_to_r=arm_to_r,
        min_n=40,
        min_edge_by_bucket={"trend": 0.05, "range": 0.08, "mixed": 0.08, "thin": 0.12},
        alpha_by_bucket={"trend": 0.10, "range": 0.10, "mixed": 0.10, "thin": 0.05},
    )
    assert dec.winner == "A"
