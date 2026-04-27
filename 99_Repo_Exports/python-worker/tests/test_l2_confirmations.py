from __future__ import annotations

from dataclasses import dataclass
import pytest

from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level, L2Snapshot
from handlers.confirmations.l2_confirmations import (
    l2_confirm_breakout,
    l2_confirm_absorption,
    VETO_WALL_NEAR,
    VETO_MP_CONTRA,
    VETO_NO_WALL_OR_REFILL,
    VETO_NO_BLOCKING_CONFIRM,
    VETO_TAKER_RATE_LOW,
    OK,
)


@dataclass
class Ctx:
    spread_bps: float = 1.0
    microprice_shift_bps_20: float = 0.0
    taker_rate_ema: float = 0.2
    refill_ratio: float = 0.0
    micro_proxy_progress_blocked: bool = False


def _l2_with_ask_wall_near(level_price: float) -> L2Snapshot:
    # wall на ask почти у level_price
    asks = [
        L2Level(price=level_price * 1.0001, size=10.0, notional=120_000.0),
        L2Level(price=level_price * 1.0010, size=2.0, notional=5_000.0),
    ]
    bids = [L2Level(price=level_price * 0.9990, size=2.0, notional=5_000.0)]
    return L2Snapshot(bids=bids, asks=asks)


def _l2_with_bid_wall_near(level_price: float) -> L2Snapshot:
    bids = [
        L2Level(price=level_price * 0.9999, size=10.0, notional=120_000.0),
        L2Level(price=level_price * 0.9990, size=2.0, notional=5_000.0),
    ]
    asks = [L2Level(price=level_price * 1.0010, size=2.0, notional=5_000.0)]
    return L2Snapshot(bids=bids, asks=asks)


def _l2_no_walls(level_price: float) -> L2Snapshot:
    bids = [L2Level(price=level_price * 0.9990, size=1.0, notional=2_000.0)]
    asks = [L2Level(price=level_price * 1.0010, size=1.0, notional=2_000.0)]
    return L2Snapshot(bids=bids, asks=asks)


def test_breakout_up_veto_wall_near() -> None:
    ctx = Ctx(spread_bps=1.0, microprice_shift_bps_20=0.0)
    level = 100.0
    l2 = _l2_with_ask_wall_near(level)
    r = l2_confirm_breakout(ctx=ctx, l2=l2, level_price=level, side="buy", wall_near_bps=6.0, min_wall_notional=50_000.0)
    assert r.veto is True
    assert r.reason_code == VETO_WALL_NEAR


def test_breakout_down_veto_wall_near() -> None:
    ctx = Ctx(spread_bps=1.0, microprice_shift_bps_20=0.0)
    level = 100.0
    l2 = _l2_with_bid_wall_near(level)
    r = l2_confirm_breakout(ctx=ctx, l2=l2, level_price=level, side="sell", wall_near_bps=6.0, min_wall_notional=50_000.0)
    assert r.veto is True
    assert r.reason_code == VETO_WALL_NEAR


def test_breakout_veto_microprice_contra() -> None:
    ctx = Ctx(spread_bps=1.0, microprice_shift_bps_20=-3.0)
    level = 100.0
    l2 = _l2_no_walls(level)
    r = l2_confirm_breakout(ctx=ctx, l2=l2, level_price=level, side="buy", mp_contra_bps=2.0)
    assert r.veto is True
    assert r.reason_code == VETO_MP_CONTRA


def test_absorption_requires_two_sources_ok() -> None:
    # src_a: wall_here=True, src_b: micro_proxy=True
    ctx = Ctx(taker_rate_ema=0.2, micro_proxy_progress_blocked=True, refill_ratio=0.0, microprice_shift_bps_20=0.0)
    level = 100.0
    l2 = _l2_with_bid_wall_near(level)
    r = l2_confirm_absorption(ctx=ctx, l2=l2, level_price=level, side="buy", wall_near_bps=8.0, min_wall_notional=75_000.0)
    assert r.veto is False
    assert r.reason_code == OK
    assert 0.0 <= r.score01 <= 1.0


def test_absorption_veto_no_wall_or_refill() -> None:
    ctx = Ctx(taker_rate_ema=0.2, micro_proxy_progress_blocked=True, refill_ratio=0.0, microprice_shift_bps_20=0.0)
    level = 100.0
    l2 = _l2_no_walls(level)
    r = l2_confirm_absorption(ctx=ctx, l2=l2, level_price=level, side="buy", wall_near_bps=8.0, min_wall_notional=75_000.0, refill_min=0.6)
    assert r.veto is True
    assert r.reason_code == VETO_NO_WALL_OR_REFILL


def test_absorption_veto_no_blocking_confirm() -> None:
    # src_a ok (wall), но src_b нет (mp_contra False, micro_proxy False)
    ctx = Ctx(taker_rate_ema=0.2, micro_proxy_progress_blocked=False, refill_ratio=0.0, microprice_shift_bps_20=0.0)
    level = 100.0
    l2 = _l2_with_bid_wall_near(level)
    r = l2_confirm_absorption(ctx=ctx, l2=l2, level_price=level, side="buy")
    assert r.veto is True
    assert r.reason_code == VETO_NO_BLOCKING_CONFIRM


def test_absorption_veto_taker_rate_low() -> None:
    ctx = Ctx(taker_rate_ema=0.001, micro_proxy_progress_blocked=True, refill_ratio=1.0, microprice_shift_bps_20=-2.0)
    level = 100.0
    l2 = _l2_with_bid_wall_near(level)
    r = l2_confirm_absorption(ctx=ctx, l2=l2, level_price=level, side="buy", min_taker_rate=0.05)
    assert r.veto is True
    assert r.reason_code == VETO_TAKER_RATE_LOW
