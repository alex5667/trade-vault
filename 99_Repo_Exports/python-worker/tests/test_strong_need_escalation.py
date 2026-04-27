from __future__ import annotations

from core.strong_need_policy import compute_strong_need_same_tick


def test_escalate_to_3_on_pressure():
    cfg = {"strong_need_reversal": 2, "strong_need_continuation": 2}
    d = compute_strong_need_same_tick(
        scenario="reversal",
        pressure_hi=True,
        churn_hi=False,
        regime="range",
        unstable=False,
        cfg=cfg,
    )
    assert d.need_rev >= 3


def test_extreme_to_4_on_thin_pressure_churn():
    cfg = {"strong_need_reversal": 2, "strong_need_continuation": 2, "strong_need_extreme_enable": 1}
    d = compute_strong_need_same_tick(
        scenario="continuation",
        pressure_hi=True,
        churn_hi=True,
        regime="thin",
        unstable=True,
        cfg=cfg,
    )
    assert d.need_cont >= 4
