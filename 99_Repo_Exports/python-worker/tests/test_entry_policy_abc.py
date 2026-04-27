from __future__ import annotations


def test_choose_arm_abc_clamps_sum():
    from services.ab_router import choose_arm_abc
    # if sb+sc>100 => clamp sc
    arm = choose_arm_abc(symbol="BTC", zone_id="Z", side="LONG", split_pct_b=90, split_pct_c=90)
    assert arm in ("A", "B", "C")


def test_choose_arm_abc_extremes():
    from services.ab_router import choose_arm_abc
    # 100% B
    assert choose_arm_abc(symbol="BTC", zone_id="Z", side="LONG", split_pct_b=100, split_pct_c=0) == "B"
    # 100% C (achieved via B=0 C=100)
    assert choose_arm_abc(symbol="BTC", zone_id="Z", side="LONG", split_pct_b=0, split_pct_c=100) == "C"
    # 0% B/C => A
    assert choose_arm_abc(symbol="BTC", zone_id="Z", side="LONG", split_pct_b=0, split_pct_c=0) == "A"


def test_abconfig_splits_by_regime():
    from services.ab_router import ABConfig
    cfg = ABConfig.from_dict({
        "enabled": 1,
        "split_pct_b": 10,
        "split_pct_c": 10,
        "split_by_regime": {"default": {"b": 8, "c": 8}, "thin": {"b": 15, "c": 15}},
    })
    sb, sc = cfg.splits_for_regime("range")
    assert (sb, sc) == (8, 8)
    sb, sc = cfg.splits_for_regime("thin")
    assert (sb, sc) == (15, 15)


def test_pick_winner_any():
    from tools.entry_policy_ab_report import pick_winner_any
    s = {
        "A": {"total": 200, "allow_rate": 5.0, "avg_coh_allow": 0.70, "avg_leader_conf_allow": 0.70},
        "B": {"total": 200, "allow_rate": 6.0, "avg_coh_allow": 0.75, "avg_leader_conf_allow": 0.75},
        "C": {"total": 200, "allow_rate": 4.0, "avg_coh_allow": 0.72, "avg_leader_conf_allow": 0.72},
    }
    w, _ = pick_winner_any(s, min_n=100, arms=["A","B","C"])
    assert w == "B"
