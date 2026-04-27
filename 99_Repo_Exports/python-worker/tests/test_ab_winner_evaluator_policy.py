from __future__ import annotations

from services.ab_winner_evaluator_core import (
    choose_winner_lcb,
    aggregate_scenario_winners,
    hysteresis_should_publish,
)


def _dec(regime: str, a: float, b: float, c: float, n: int = 60):
    arm_to_r = {"A": [a] * n, "B": [b] * n, "C": [c] * n}
    return choose_winner_lcb(
        regime=regime,
        arm_to_r=arm_to_r,
        min_n=40,
        min_edge_by_bucket={"trend": 0.05, "range": 0.08, "mixed": 0.08, "thin": 0.12},
        alpha_by_bucket={"trend": 0.10, "range": 0.10, "mixed": 0.10, "thin": 0.05},
        require_lcb_gt0_for_non_a=True,
    )


def test_aggregate_scenarios_agree_non_a_ok():
    pooled = _dec("range", a=0.02, b=0.20, c=0.05)
    cont = _dec("range", a=0.02, b=0.20, c=0.05)
    rev = _dec("range", a=0.02, b=0.20, c=0.05)
    out = aggregate_scenario_winners(
        regime="range",
        pooled=pooled,
        per_scn={"continuation": cont, "reversal": rev},
        require_same_winner_when_non_a=True,
        disagree_allow_margin_r=0.18,
    )
    assert out.winner == "B"


def test_aggregate_scenarios_disagree_fallback_a():
    pooled = _dec("range", a=0.02, b=0.20, c=0.05)
    cont = _dec("range", a=0.02, b=0.20, c=0.05)  # B
    rev = _dec("range", a=0.15, b=0.05, c=0.05)   # A
    out = aggregate_scenario_winners(
        regime="range",
        pooled=pooled,
        per_scn={"continuation": cont, "reversal": rev},
        require_same_winner_when_non_a=True,
        disagree_allow_margin_r=0.50,  # make it hard to pass
    )
    assert out.winner == "A"


def test_hysteresis_hold_down_blocks_a_to_b():
    pooled = _dec("range", a=0.02, b=0.20, c=0.05)
    prev = {"winner_arm": "A", "ts_ms": 1000}
    ok, why = hysteresis_should_publish(
        now_ms=1000 + 10_000,
        prev_meta=prev,
        new_winner=pooled,
        hold_down_ms=60_000,
        switch_min_margin_r=0.12,
    )
    assert ok is False
    assert why == "hold_down"


def test_hysteresis_revert_to_a_allowed():
    prev = {"winner_arm": "B", "ts_ms": 1000}
    pooled = _dec("range", a=0.20, b=0.01, c=0.01)  # A
    ok, why = hysteresis_should_publish(
        now_ms=1000 + 10_000,
        prev_meta=prev,
        new_winner=pooled,
        hold_down_ms=999_999,
        switch_min_margin_r=0.12,
    )
    assert ok is True
    assert why == "revert_to_A"
