from __future__ import annotations

from core.candidate_score import compute_candidate_score


def test_churn_penalizes_score():
    cfg = {"cand_p_book_churn": 0.2, "book_rate_z_penalty_start": 2.0, "book_rate_z_penalty_full": 5.0}
    base = dict(
        of_confirm_score=1.0,
        delta_z=2.0,
        confirmations=["obi_stable=2.0s"],
        spread_z=0.0,
        spread_bps=2.0,
        book_stale_ms=100,
        cfg=cfg,
        pressure_hi=False,
    )
    s0 = compute_candidate_score(book_rate_z=0.5, **base)
    s1 = compute_candidate_score(book_rate_z=4.0, **base)
    assert s1.score < s0.score


def test_pressure_veto_churn():
    cfg = {"pressure_book_rate_z_max": 6.0}
    s = compute_candidate_score(
        of_confirm_score=1.0,
        delta_z=2.0,
        confirmations=[],
        spread_z=0.0,
        spread_bps=2.0,
        book_stale_ms=100,
        book_rate_z=7.0,
        cfg=cfg,
        pressure_hi=True,
    )
    assert s.veto == "PRESSURE_VETO_BOOK_CHURN"
