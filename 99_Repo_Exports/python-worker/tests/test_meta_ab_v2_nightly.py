import pytest
from tools.meta_ab_winner_evaluator_v2 import recommend_next_share, V2Config

def test_recommend_next_share():
    cfg = V2Config(
        p_min=0.55, label_col="y", r_col="r_mult", ok_col="ok",
        min_n=1000, min_delta_exp_r=0.002, tail_r=-1.0, tail_slack=0.01,
        bootstrap=1, boot_n=400, boot_alpha=0.1, boot_seed=1337,
        require_ci_positive=1, strata_cols=("symbol",), strata_topk=10,
        current_share=0.0,
        ramp_step=0.05, max_share=0.50
    )

    # 1. Challenger wins -> increase share
    next_s, act = recommend_next_share("challenger", 0.10, cfg, None)
    assert next_s == pytest.approx(0.15)
    assert act == "increase_share"

    # 2. Champion wins -> decrease share
    next_s, act = recommend_next_share("champion", 0.15, cfg, None)
    assert next_s == pytest.approx(0.10)
    assert act == "decrease_share"

    # 3. Tie -> hold
    next_s, act = recommend_next_share("tie", 0.10, cfg, None)
    assert next_s == pytest.approx(0.10)
    assert act == "hold"

    # 4. Cap by config max_share
    next_s, act = recommend_next_share("challenger", 0.48, cfg, None)
    assert next_s == pytest.approx(0.50)
    assert act == "increase_share"

    # 5. Cap by freeze_max (lower than config max_share)
    next_s, act = recommend_next_share("challenger", 0.20, cfg, 0.22)
    assert next_s == pytest.approx(0.22)
    assert act == "increase_share"

    # 6. Cannot drop below 0
    next_s, act = recommend_next_share("champion", 0.02, cfg, None)
    assert next_s == pytest.approx(0.0)
    assert act == "decrease_share"
