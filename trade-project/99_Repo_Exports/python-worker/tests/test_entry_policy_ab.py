from __future__ import annotations


def test_ab_router_stable_bucket():
    from services.ab_router import stable_bucket_0_99
    assert stable_bucket_0_99("BTC|X|LONG") == stable_bucket_0_99("BTC|X|LONG")
    b = stable_bucket_0_99("BTC|X|LONG")
    assert 0 <= b <= 99


def test_ab_router_choose_arm_split():
    from services.ab_router import choose_arm
    # if split_pct_b=100 => always B
    assert choose_arm(symbol="BTC", zone_id="Z", side="LONG", split_pct_b=100) == "B"
    # if split_pct_b=0 => always A
    assert choose_arm(symbol="BTC", zone_id="Z", side="LONG", split_pct_b=0) == "A"


def test_ab_report_pick_winner_insufficient():
    from tools.entry_policy_ab_report import pick_winner
    s = {"A": {"total": 10}, "B": {"total": 10}}
    w, r = pick_winner(s, min_n=100)
    assert w == "NA"


def test_ab_report_pick_winner_prefers_quality():
    from tools.entry_policy_ab_report import pick_winner
    s = {
        "A": {"total": 200, "allow_rate": 5.0, "avg_coh_allow": 0.70, "avg_leader_conf_allow": 0.70},
        "B": {"total": 200, "allow_rate": 6.0, "avg_coh_allow": 0.75, "avg_leader_conf_allow": 0.75},
    }
    w, _ = pick_winner(s, min_n=100)
    assert w == "B"
