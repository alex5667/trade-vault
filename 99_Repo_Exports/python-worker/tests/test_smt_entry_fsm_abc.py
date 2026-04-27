from __future__ import annotations

from services.smt_entry_abc_config import ArmPolicy
from services.smt_entry_candidate_service import RetestState, _fsm_step


class Setup:
    def __init__(self, kind: str, trend_dir: str, div: str):
        self.kind = kind
        self.trend_dir = trend_dir
        self.div = div
        self.leader = "BTC"
        self.pick = "ETH"
        self.bundle = "bundle1"


def test_fsm_requires_of_score():
    setup = Setup(kind="continuation", trend_dir="UP", div="none")
    st = RetestState(stage="WAIT_RETEST", zone_id="Z1")
    snap = {
        "close_px": 100.0,
        "zone_id": "Z1",
        "zone_px_lo": 99.0,
        "zone_px_hi": 101.0,
        "zone_dist_bp": 5.0,
        "of_strong": 1,
        "of_dir": "LONG",
        "of_confirm_score": 0.5,
        "zone_side": "MID",
        "regime": "range",
        "abs_lvl_th_unstable": 0,
    }
    pol = ArmPolicy(min_of_score=1.0, obi_min_sec=1.5)
    emit, reason = _fsm_step(setup=setup, st=st, snap=snap, now_ms=1, touch_bp=10, away_bp=25, retest_bp=10, pol=pol)
    assert emit is False
    assert reason == "of_score_low"


def test_fsm_thin_requires_obi_or_ice():
    setup = Setup(kind="continuation", trend_dir="UP", div="none")
    st = RetestState(stage="WAIT_RETEST", zone_id="Z1")
    snap = {
        "close_px": 100.0,
        "zone_id": "Z1",
        "zone_px_lo": 99.0,
        "zone_px_hi": 101.0,
        "zone_dist_bp": 5.0,
        "of_strong": 1,
        "of_dir": "LONG",
        "of_confirm_score": 1.0,
        "zone_side": "MID",
        "regime": "thin",
        "abs_lvl_th_unstable": 0,
        "obi_stable_sec": 0.5,
        "iceberg_strict": 0,
    }
    pol = ArmPolicy(min_of_score=1.0, obi_min_sec=1.5)
    emit, reason = _fsm_step(setup=setup, st=st, snap=snap, now_ms=1, touch_bp=10, away_bp=25, retest_bp=10, pol=pol)
    assert emit is False
    assert reason == "thin_need_obi_or_ice"
