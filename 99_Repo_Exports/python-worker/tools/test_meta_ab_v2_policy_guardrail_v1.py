from __future__ import annotations

from dataclasses import dataclass

from services.orderflow.meta_ab_v2_policy_guardrail_v1 import decide_meta_ab_v2_policy


@dataclass
class DummyCfg:
    min_n: int = 1000
    min_delta_exp_r: float = 0.002
    tail_slack: float = 0.01
    ramp_step: float = 0.05
    max_share: float = 0.50


def _rep(winner="challenger", n_eligible=2000, d_expr=0.003, d_tail=0.0, ci_lo=0.001, reason=""):
    rep = {
        "winner": winner,
        "reason": reason,
        "counts": {"n_eligible": n_eligible, "n_total": n_eligible},
        "delta": {"exp_r_per_candidate": d_expr, "tail_rate_per_candidate": d_tail},
        "ci": {"delta_exp_r_lo": ci_lo} if ci_lo is not None else {},
    }
    return rep


def test_increase_allowed_when_all_good():
    cfg = DummyCfg()
    rep = _rep()
    dec = decide_meta_ab_v2_policy(
        rep=rep,
        cfg=cfg,
        share_current=0.10,
        share_next_raw=0.15,
        action_raw="increase_share",
        freeze_max_share=None,
        env_overrides={"enabled": True, "fail_closed": True, "allow_decrease": True},
    )
    assert dec.blocked is False
    assert dec.allow_apply is True
    assert dec.action_final == "increase_share"
    assert abs(dec.share_next_final - 0.15) < 1e-9


def test_increase_blocked_if_ci_missing_fail_closed():
    cfg = DummyCfg()
    rep = _rep(ci_lo=None)
    dec = decide_meta_ab_v2_policy(
        rep=rep,
        cfg=cfg,
        share_current=0.10,
        share_next_raw=0.15,
        action_raw="increase_share",
        freeze_max_share=None,
        env_overrides={"enabled": True, "fail_closed": True, "allow_decrease": True, "require_ci_positive_for_increase": True},
    )
    assert dec.blocked is True
    assert dec.allow_apply is False
    assert dec.action_final == "hold"
    assert abs(dec.share_next_final - 0.10) < 1e-9
    assert "ci_missing" in dec.reasons


def test_increase_blocked_if_winner_not_challenger():
    cfg = DummyCfg()
    rep = _rep(winner="tie")
    dec = decide_meta_ab_v2_policy(
        rep=rep,
        cfg=cfg,
        share_current=0.10,
        share_next_raw=0.15,
        action_raw="increase_share",
        freeze_max_share=None,
        env_overrides={"enabled": True, "fail_closed": True},
    )
    assert dec.blocked is True
    assert dec.action_final == "hold"
    assert "winner_not_challenger" in dec.reasons


def test_decrease_disallowed_blocks_change():
    cfg = DummyCfg()
    rep = _rep(winner="champion", d_expr=-0.002, ci_lo=-0.001)
    dec = decide_meta_ab_v2_policy(
        rep=rep,
        cfg=cfg,
        share_current=0.20,
        share_next_raw=0.15,
        action_raw="decrease_share",
        freeze_max_share=None,
        env_overrides={"enabled": True, "fail_closed": True, "allow_decrease": False},
    )
    assert dec.blocked is True
    assert dec.action_final == "hold"
    assert "decrease_disallowed" in dec.reasons
