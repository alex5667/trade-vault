import pytest
from core.candidate_score import compute_candidate_score

def test_candidate_score_penalties():
    cfg = {
        "cand_w_of": 1.0,
        "cand_w_delta_z": 0.5,
        "cand_b_obi": 0.2,
        "spread_z_penalty_start": 2.0,
        "spread_z_penalty_full": 5.0,
        "cand_p_spread_z": 0.25,
        "book_stale_penalty_start_ms": 1000,
        "book_stale_penalty_full_ms": 5000,
        "cand_p_book_stale": 0.20,
    }
    
    # Case 1: Perfect conditions
    # Base = 1.0(of) + 0.5(delta_z) + 0.2(obi) = 1.7
    cs1 = compute_candidate_score(
        of_confirm_score=1.0,
        delta_z=3.0,
        confirmations=["obi_stable=1"],
        spread_z=0.5,
        spread_bps=1.0,
        book_stale_ms=100,
        book_rate_z=0.0,
        cfg=cfg,
        pressure_hi=False
    )
    assert cs1.score == pytest.approx(1.7)
    assert cs1.veto == ""

    # Case 2: High Spread Z penalty
    # spread_z = 3.5, midway between 2.0 and 5.0
    # Penalty factor = (3.5 - 2.0)/(5.0 - 2.0) = 0.5
    # Penalty = 0.5 * 0.25 = 0.125
    # Base = 1.0 (of) + 0.0 + 0.0 = 1.0
    # Score = 1.0 - 0.125 = 0.875
    cs2 = compute_candidate_score(
        of_confirm_score=1.0,
        delta_z=0.0,
        confirmations=[],
        spread_z=3.5,
        spread_bps=5.0,
        book_stale_ms=100,
        book_rate_z=0.0,
        cfg=cfg,
        pressure_hi=False
    )
    assert cs2.score == pytest.approx(0.875)

    # Case 3: High Staleness penalty
    # book_stale_ms = 3000, midway between 1000 and 5000
    # Penalty factor = (3000-1000)/(5000-1000) = 0.5
    # Penalty = 0.5 * 0.20 = 0.10
    # Base = 1.0
    # Score = 1.0 - 0.10 = 0.90
    cs3 = compute_candidate_score(
        of_confirm_score=1.0,
        delta_z=0.0,
        confirmations=[],
        spread_z=0.0,
        spread_bps=1.0,
        book_stale_ms=3000,
        book_rate_z=0.0,
        cfg=cfg,
        pressure_hi=False
    )
    assert cs3.score == pytest.approx(0.90)

def test_candidate_score_veto_under_pressure():
    cfg = {
        "pressure_spread_z_max": 3.0,
        "pressure_book_stale_max_ms": 2000,
        "spread_z_penalty_start": 2.0,
        "spread_z_penalty_full": 5.0,
        "cand_p_spread_z": 0.25,
        "book_stale_penalty_start_ms": 1000,
        "book_stale_penalty_full_ms": 5000,
        "cand_p_book_stale": 0.20,
    }
    
    # Spread Z veto
    # spread_z = 3.1 => penalty factor = (3.1-2.0)/3.0 = 0.3666
    # penalty = 0.3666 * 0.25 = 0.09166
    # score = 1.0 - 0.09166 = 0.90833
    cs_v1 = compute_candidate_score(
        of_confirm_score=1.0, delta_z=0.0, confirmations=[],
        spread_z=3.1, spread_bps=1.0, book_stale_ms=100,
        book_rate_z=0.0,
        cfg=cfg, pressure_hi=True
    )
    assert cs_v1.veto == "PRESSURE_VETO_SPREAD_Z"
    assert cs_v1.score == pytest.approx(0.90833, abs=1e-4)

    # Book stale veto
    # book_stale_ms = 2500 => penalty factor = (2500-1000)/4000 = 0.375
    # penalty = 0.375 * 0.20 = 0.075
    # score = 1.0 - 0.075 = 0.925
    cs_v2 = compute_candidate_score(
        of_confirm_score=1.0, delta_z=0.0, confirmations=[],
        spread_z=1.0, spread_bps=1.0, book_stale_ms=2500,
        book_rate_z=0.0,
        cfg=cfg, pressure_hi=True
    )
    assert cs_v2.veto == "PRESSURE_VETO_BOOK_STALE"
    assert cs_v2.score == pytest.approx(0.925)
