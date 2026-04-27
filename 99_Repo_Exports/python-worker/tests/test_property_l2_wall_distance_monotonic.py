from __future__ import annotations
from utils.time_utils import get_ny_time_millis

from dataclasses import dataclass
import random
import time

from handlers.confirmations.l2_confirmations import L2ConfirmBreakout
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level

@dataclass
class L2Snap:
    ts_ms: int
    bids: list[L2Level]
    asks: list[L2Level]


@dataclass
class Ctx:
    ts: int
    price: float
    side: int = 1


def _mk_l2_with_wall(ref: float, dist_bps: float, now_ms: int) -> L2Snap:
    # wall на bid стороне: price = ref * (1 - dist_bps/10000)
    wall_price = ref * (1.0 - dist_bps / 10_000.0)
    return L2Snap(
        ts_ms=now_ms,
        bids=[
            L2Level(price=ref - 0.05, size=1.0, notional=1000.0),
            L2Level(price=wall_price, size=5.0, notional=50000.0),
        ],
        asks=[
            L2Level(price=ref + 0.05, size=1.0, notional=1000.0),
            L2Level(price=ref + 0.10, size=1.0, notional=900.0),
        ],
    )


def test_property_wall_distance_score_monotonic_nonincreasing():
    conf = L2ConfirmBreakout()
    now = get_ny_time_millis()
    ctx = Ctx(ts=now, price=100.0, side=1)

    # property-based style без Hypothesis: много рандомных прогонов
    for seed in range(30):
        random.seed(seed)
        dists = sorted([random.uniform(0.5, 45.0) for _ in range(12)])
        scores = []
        for d in dists:
            l2 = _mk_l2_with_wall(100.0, d, now)
            r = conf.confirm(ctx=ctx, l2=l2, level_price=100.0)
            scores.append(0.0 if r.veto else float(r.score01))
        # score должен НЕ возрастать при увеличении dist (с допуском на veto->0)
        for i in range(1, len(scores)):
            assert scores[i] <= scores[i - 1] + 1e-9


def test_property_nan_inf_do_not_crash():
    conf = L2ConfirmBreakout()
    now = get_ny_time_millis()
    ctx = Ctx(ts=now, price=100.0, side=1)
    l2 = L2Snap(
        ts_ms=now,
        bids=[L2Level(price=float("nan"), size=1.0, notional=1000.0), L2Level(price=99.9, size=1.0, notional=float("inf"))],
        asks=[L2Level(price=100.1, size=1.0, notional=1000.0)],
    )
    r = conf.confirm(ctx=ctx, l2=l2, level_price=100.0)
    assert r is not None
