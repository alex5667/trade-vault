from __future__ import annotations

from dataclasses import dataclass

from common.qf_codes import QF
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level
from handlers.pipeline.candidate import Candidate
from handlers.pipeline.pipeline import SignalPipeline


@dataclass
class L2Snap:
    ts_ms: int
    bids: list[L2Level]
    asks: list[L2Level]


@dataclass
class Ctx:
    ts: int
    price: float
    # L2/L3 hooks used by ConfirmationsEngine
    l2: object | None = None
    l3: object | None = None
    geometry_score: float | None = 1.0
    # breakout/obi fields
    taker_rate_ema: float | None = None
    microprice_shift_bps_20: float | None = None
    cancel_to_trade_bid_5s: float | None = None
    cancel_to_trade_ask_5s: float | None = None
    cancel_to_trade_bid_20s: float | None = None
    cancel_to_trade_ask_20s: float | None = None
    spread_bps: float | None = None
    obi_sustained: bool = False
    # absorption flags
    wall_here: bool = False
    refill: bool = False
    mp_contra: bool = False
    micro_proxy: bool = False


def _mk_book(ts: int, *, mid: float = 100.0, wall_notional: float = 25000.0) -> L2Snap:
    bb = mid - 0.05
    ba = mid + 0.05
    bids = [
        L2Level(price=bb, size=1.0, notional=800.0),
        L2Level(price=bb - 0.1, size=1.0, notional=900.0),
        L2Level(price=bb - 0.2, size=1.0, notional=wall_notional),
    ]
    asks = [
        L2Level(price=ba, size=1.0, notional=800.0),
        L2Level(price=ba + 0.1, size=1.0, notional=900.0),
        L2Level(price=ba + 0.2, size=1.0, notional=950.0),
    ]
    return L2Snap(ts_ms=ts, bids=bids, asks=asks)


def test_breakout_fake_breakout_penalty_reduces_conf():
    pipe = SignalPipeline()
    ctx = Ctx(
        ts=100_000,
        price=100.0,
        l2=_mk_book(100_000, mid=100.0),
        geometry_score=1.0,
        taker_rate_ema=0.10,
        microprice_shift_bps_20=2.0,  # big shift
        cancel_to_trade_bid_5s=0.5,
        spread_bps=2.0,
    )
    cand = Candidate(kind="breakout", side=1, raw_score=2.0, level_price=100.0)
    r = pipe.validate_and_score(ctx=ctx, cand=cand)
    assert r.veto is False
    assert int(QF.BO_CONTINUATION_PENALTY) in r.quality_codes or "bo_fake_breakout_penalty" in r.parts
    assert 0.0 <= r.parts["conf_factor01"] <= 1.0


def test_breakout_spoof_veto():
    pipe = SignalPipeline()
    ctx = Ctx(
        ts=100_000,
        price=100.0,
        l2=_mk_book(100_000, mid=100.0),
        geometry_score=1.0,
        taker_rate_ema=0.10,
        microprice_shift_bps_20=2.0,
        cancel_to_trade_bid_5s=9.0,  # huge
        spread_bps=2.0,
    )
    cand = Candidate(kind="breakout", side=1, raw_score=2.0, level_price=100.0)
    r = pipe.validate_and_score(ctx=ctx, cand=cand)
    assert r.veto is True
    assert int(QF.BO_FAKE_BREAKOUT_VETO) in r.quality_codes


def test_breakout_success():
    pipe = SignalPipeline()
    ctx = Ctx(
        ts=100_000,
        price=100.0,
        l2=_mk_book(100_000, mid=100.0),
        geometry_score=1.0,
        taker_rate_ema=0.80, # good taker
        microprice_shift_bps_20=0.5, # low shift
        cancel_to_trade_bid_5s=1.0, # low c2t
        spread_bps=2.0,
    )
    cand = Candidate(kind="breakout", side=1, raw_score=2.0, level_price=100.0)
    r = pipe.validate_and_score(ctx=ctx, cand=cand)
    assert r.veto is False
    assert "bo_cancel_to_trade" in r.parts


def test_absorption_requires_two_sources_and_taker():
    pipe = SignalPipeline()
    ctx = Ctx(
        ts=100_000,
        price=100.0,
        l2=_mk_book(100_000, mid=100.0, wall_notional=60000.0),
        geometry_score=1.0,
        taker_rate_ema=0.60,
        wall_here=True,
        refill=False,
        mp_contra=False,
        micro_proxy=False,  # missing second group -> veto
    )
    cand = Candidate(kind="absorption", side=1, raw_score=2.5, level_price=100.0)
    r = pipe.validate_and_score(ctx=ctx, cand=cand)
    assert r.veto is True
    assert int(QF.AB_NEED_2OF2_VETO) in r.quality_codes or "ab_need_2of2_veto" in r.parts


def test_obi_spike_spoof_veto_when_high_c2t_and_not_sustained():
    pipe = SignalPipeline()
    ctx = Ctx(
        ts=100_000,
        price=100.0,
        l2=_mk_book(100_000, mid=100.0, wall_notional=60000.0),
        geometry_score=1.0,
        obi_sustained=False,
        cancel_to_trade_bid_5s=9.0,
        spread_bps=5.0,
    )
    cand = Candidate(kind="obi_spike", side=1, raw_score=1.8, level_price=None)
    r = pipe.validate_and_score(ctx=ctx, cand=cand)
    assert r.veto is True
    assert int(QF.OBI_SPOOF_CANCEL_PENALTY) in r.quality_codes or "obi_spoof_veto" in r.parts


def test_absorption_low_taker_veto():
    pipe = SignalPipeline()
    ctx = Ctx(
        ts=100_000,
        price=100.0,
        l2=_mk_book(100_000, mid=100.0, wall_notional=60000.0),
        wall_here=True,
        refill=True,
        mp_contra=True,
        micro_proxy=True,
        taker_rate_ema=0.10,  # low taker
    )
    cand = Candidate(kind="absorption", side=1, raw_score=2.0, level_price=100.0)
    r = pipe.validate_and_score(ctx=ctx, cand=cand)
    assert r.veto is True
    assert int(QF.AB_LOW_TAKER_VETO) in r.quality_codes


def test_absorption_success():
    pipe = SignalPipeline()
    ctx = Ctx(
        ts=100_000,
        price=100.0,
        l2=_mk_book(100_000, mid=100.0, wall_notional=60000.0),
        wall_here=True,
        refill=False,
        mp_contra=False,
        micro_proxy=True,
        taker_rate_ema=0.60,
    )
    cand = Candidate(kind="absorption", side=1, raw_score=2.0, level_price=100.0)
    r = pipe.validate_and_score(ctx=ctx, cand=cand)
    assert r.veto is False


def test_obi_spike_spread_scale():
    pipe = SignalPipeline()
    ctx = Ctx(
        ts=100_000,
        price=100.0,
        l2=_mk_book(100_000, mid=100.0),
        obi_sustained=True,
        cancel_to_trade_bid_5s=1.0,
        spread_bps=20.0,
    )
    cand = Candidate(kind="obi_spike", side=1, raw_score=2.0, level_price=100.0)
    r = pipe.validate_and_score(ctx=ctx, cand=cand)
    assert r.veto is False
    assert "obi_spread_scale" in r.parts
    assert r.parts["obi_spread_scale"] < 1.0


def test_obi_spike_sustained_penalty():
    pipe = SignalPipeline()
    ctx = Ctx(
        ts=100_000,
        price=100.0,
        l2=_mk_book(100_000),
        obi_sustained=False,
        cancel_to_trade_bid_5s=1.0,
        spread_bps=2.0,
    )
    cand = Candidate(kind="obi_spike", side=1, raw_score=2.0, level_price=100.0)
    r = pipe.validate_and_score(ctx=ctx, cand=cand)
    assert r.veto is False
    assert int(QF.OBI_NOT_SUSTAINED_PENALTY) in r.quality_codes


def test_obi_spike_high_c2t_penalty():
    pipe = SignalPipeline()
    ctx = Ctx(
        ts=100_000,
        price=100.0,
        l2=_mk_book(100_000),
        obi_sustained=True,
        cancel_to_trade_bid_5s=5.0,
        spread_bps=2.0,
    )
    cand = Candidate(kind="obi_spike", side=1, raw_score=2.0, level_price=100.0)
    r = pipe.validate_and_score(ctx=ctx, cand=cand)
    assert r.veto is False
    assert int(QF.OBI_HIGH_CANCEL_TO_TRADE_PENALTY) in r.quality_codes
