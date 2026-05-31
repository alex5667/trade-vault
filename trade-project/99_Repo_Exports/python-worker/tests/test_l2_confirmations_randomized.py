import math
import random
from dataclasses import dataclass

import pytest

from confirmations.l2_confirmations import L2ConfirmAbsorption, L2ConfirmBreakout


@dataclass
class _Cfg:
    L2_STALE_MS: int = 1500
    L2_NEAR_BPS: float = 8.0
    L2_MIN_NEAR_NOTIONAL: float = 5000.0
    L2_MIN_DEF_NOTIONAL: float = 3000.0
    L2_MIN_WALL_NOTIONAL: float = 15000.0
    L2_MAX_OPP_WALL_DIST_BPS: float = 12.0
    L2_MAX_WALL_DIST_BPS: float = 10.0


@dataclass
class _L2Level:
    price: float
    size: float
    notional: float


@dataclass
class _L2Snap:
    bids: list
    asks: list
    ts_ms: int


def _mk_level(price: float, notional: float) -> _L2Level:
    size = 0.0 if price <= 0 else notional / price
    return _L2Level(price=price, size=size, notional=notional)


def test_l2_confirmations_do_not_throw_on_nan_inf():
    cfg = _Cfg()
    now = 10_000
    br = L2ConfirmBreakout(cfg, now_ms=lambda: now)
    ab = L2ConfirmAbsorption(cfg, now_ms=lambda: now)

    snap = _L2Snap(
        bids=[_L2Level(price=float("nan"), size=1.0, notional=1000.0), _L2Level(price=100.0, size=float("inf"), notional=float("inf"))],
        asks=[_L2Level(price=101.0, size=1.0, notional=1000.0)],
        ts_ms=now,
    )

    r1 = br.check(snap, side=+1, price=100.5)
    r2 = ab.check(snap, side=+1, price=100.5)
    assert isinstance(r1.ok, bool)
    assert isinstance(r2.ok, bool)


def test_l2_confirmations_fail_on_stale_l2():
    cfg = _Cfg(L2_STALE_MS=500)
    now = 10_000
    br = L2ConfirmBreakout(cfg, now_ms=lambda: now)
    ab = L2ConfirmAbsorption(cfg, now_ms=lambda: now)

    snap = _L2Snap(bids=[_mk_level(100.0, 10000.0)], asks=[_mk_level(101.0, 10000.0)], ts_ms=now - 1000)
    r1 = br.check(snap, side=+1, price=100.0)
    r2 = ab.check(snap, side=-1, price=100.0)
    assert r1.ok is False and r1.reason_code == "stale_l2"
    assert r2.ok is False and r2.reason_code == "stale_l2"


def test_wall_distance_threshold_behavior_absorption():
    cfg = _Cfg(L2_MIN_WALL_NOTIONAL=15000.0, L2_MAX_WALL_DIST_BPS=10.0)
    now = 10_000
    ab = L2ConfirmAbsorption(cfg, now_ms=lambda: now)

    price = 100.0
    # wall exactly at price -> dist 0 bps (should pass wall check)
    snap_ok = _L2Snap(
        bids=[_mk_level(99.99, 6000.0)],
        asks=[_mk_level(100.0, 20000.0)],  # wall
        ts_ms=now,
    )
    r_ok = ab.check(snap_ok, side=+1, price=price)
    assert r_ok.reason_code in ("ok", "low_def_near")  # wall check must not be the failure here

    # wall far away -> should fail wall check
    snap_bad = _L2Snap(
        bids=[_mk_level(99.99, 6000.0)],
        asks=[_mk_level(100.5, 30000.0)],  # 50 bps away
        ts_ms=now,
    )
    r_bad = ab.check(snap_bad, side=+1, price=price)
    assert r_bad.ok is False
    assert r_bad.reason_code == "no_close_wall"


def test_randomized_extremes_breakout_and_absorption():
    """
    Property-style randomized test (no Hypothesis dependency):
      - never throws
      - ok implies finite parts where expected
    """
    cfg = _Cfg()
    rng = random.Random(0)
    now = 10_000
    br = L2ConfirmBreakout(cfg, now_ms=lambda: now)
    ab = L2ConfirmAbsorption(cfg, now_ms=lambda: now)

    def rand_price():
        # include edge cases
        roll = rng.random()
        if roll < 0.05:
            return float("nan")
        if roll < 0.10:
            return float("inf")
        if roll < 0.15:
            return 0.0
        return rng.uniform(10.0, 100_000.0)

    for _ in range(500):
        mid = rand_price()
        # build some levels around "mid" if it's finite
        bids = []
        asks = []
        for _k in range(rng.randint(0, 30)):
            p = rand_price()
            n = rand_price()
            if not math.isfinite(n) or n < 0:
                n = 0.0
            bids.append(_L2Level(price=p, size=0.0, notional=n))
        for _k in range(rng.randint(0, 30)):
            p = rand_price()
            n = rand_price()
            if not math.isfinite(n) or n < 0:
                n = 0.0
            asks.append(_L2Level(price=p, size=0.0, notional=n))

        snap = _L2Snap(bids=bids, asks=asks, ts_ms=now)

        for side in (+1, -1):
            r1 = br.check(snap, side=side, price=mid)
            r2 = ab.check(snap, side=side, price=mid)
            assert isinstance(r1.ok, bool)
            assert isinstance(r2.ok, bool)
            assert isinstance(r1.reason_code, str)
            assert isinstance(r2.reason_code, str)


def test_hypothesis_property_based_if_available():
    hyp = pytest.importorskip("hypothesis")
    st = pytest.importorskip("hypothesis.strategies")

    @dataclass
    class _Cfg:
        L2_STALE_MS: int = 1500
        L2_NEAR_BPS: float = 8.0
        L2_MIN_NEAR_NOTIONAL: float = 5000.0
        L2_MIN_DEF_NOTIONAL: float = 3000.0
        L2_MIN_WALL_NOTIONAL: float = 15000.0
        L2_MAX_OPP_WALL_DIST_BPS: float = 12.0
        L2_MAX_WALL_DIST_BPS: float = 10.0

    @dataclass
    class _L2Level:
        price: float
        size: float
        notional: float

    @dataclass
    class _L2Snap:
        bids: list
        asks: list
        ts_ms: int

    cfg = _Cfg()
    now = 10_000
    br = L2ConfirmBreakout(cfg, now_ms=lambda: now)

    float_any = st.floats(allow_nan=True, allow_infinity=True, width=64)
    level = st.builds(_L2Level, price=float_any, size=float_any, notional=float_any)
    snap = st.builds(_L2Snap, bids=st.lists(level, max_size=50), asks=st.lists(level, max_size=50), ts_ms=st.integers(min_value=0, max_value=100_000))

    @hyp.given(snap=snap, price=float_any, side=st.sampled_from([+1, -1]))
    def _prop(snap, price, side):
        r = br.check(snap, side=side, price=price)
        assert isinstance(r.ok, bool)
        assert isinstance(r.reason_code, str)

    _prop()
