from __future__ import annotations

from dataclasses import dataclass
from services.smt_logic import decide_smt


@dataclass
class Snap:
    symbol: str
    ts_ms: int = 1
    trend_dir: str = "UP"
    of_dir: str = "LONG"
    close_cross: int = 1
    of_strong: int = 1
    reclaim: int = 0
    reclaim_dir: str = "NONE"
    sweep: int = 0
    sweep_dir: str = "NONE"
    weak_progress: int = 0
    div_kind: str = "none"
    delta_z: float = 3.0
    zone_dist_bp: float = 5.0
    # swings
    swing_low_0: float = 90
    swing_low_1: float = 100
    swing_high_0: float = 0
    swing_high_1: float = 0
    # rank features
    rsi14: float = 60
    cvd_slope: float = 1.0
    retrace_atr: float = 0.1


def test_confirm_requires_conf_score():
    leader = Snap(symbol="BTCUSDT", zone_dist_bp=1000.0)  # far => confScore low
    s1 = Snap(symbol="ETHUSDT")
    s2 = Snap(symbol="SOLUSDT")
    dec = decide_smt(leader, [leader, s1, s2], coh=0.9, cfg={"smt_coh_threshold": 0.65, "smt_leader_conf_min_score": 0.9, "smt_rank_mode": "cross"})
    assert dec.kind == "none"


def test_basket_smt_requires_k():
    leader = Snap(symbol="BTCUSDT", close_cross=0, of_strong=0, sweep=1, reclaim=1, weak_progress=1, sweep_dir="LONG", trend_dir="UP")
    s1 = Snap(symbol="ETHUSDT", swing_low_0=105, swing_low_1=100)  # HL
    s2 = Snap(symbol="SOLUSDT", swing_low_0=106, swing_low_1=100)  # HL
    dec = decide_smt(leader, [leader, s1, s2], coh=0.5, cfg={"smt_basket_k": 2, "smt_rank_mode": "cross"})
    assert dec.kind == "reversal"
    assert dec.div == "bullish_smt"
