from __future__ import annotations

from types import SimpleNamespace

from handlers.confirmations.l2_confirm_breakout import BreakoutConfirmCfg, L2ConfirmBreakout
from handlers.confirmations.l2_confirmations import OK, VETO_WALL_NEAR, l2_confirm_breakout
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level, L2Snapshot


def _mk_l2(*, bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> L2Snapshot:
    """
    Helper: builds L2Snapshot from (price, notional) pairs.
    size is derived (notional/price) just to satisfy L2Level fields.
    """
    bb = [L2Level(price=p, size=(n / p if p else 0.0), notional=n) for (p, n) in bids]
    aa = [L2Level(price=p, size=(n / p if p else 0.0), notional=n) for (p, n) in asks]
    return L2Snapshot(bids=bb, asks=aa)


def test_class_breakout_wall_near_is_veto_buy():
    """
    Variant B: class L2ConfirmBreakout MUST veto on near_big_wall.
    This is the source of truth for VETO_WALL_NEAR after unification.
    """
    cfg = BreakoutConfirmCfg(
        l2_stale_ms=1500,
        min_wall_notional=25_000.0,
        max_near_wall_bps=4.0,
    )
    v = L2ConfirmBreakout(cfg=cfg)

    lvl = 100.00
    # nearest ask wall very close: 2 bps away, notional big => veto
    l2 = _mk_l2(
        bids=[(99.99, 10_000.0)],
        asks=[(100.02, 60_000.0), (100.20, 10_000.0)],
    )

    ctx = SimpleNamespace(
        price=100.01,
        ts_ms=10_000,
        l2_ts_ms=9_900,  # fresh
    )

    r = v.confirm(ctx=ctx, side="buy", level_price=lvl, l2=l2)
    assert r.veto is True
    assert r.reason_code == VETO_WALL_NEAR
    assert float(r.score01) == 0.0
    # sanity: parts should carry distances/notional for debugging
    assert "near_wall_bps" in r.parts
    assert "near_wall_notional" in r.parts


def test_class_breakout_wall_near_is_veto_sell():
    cfg = BreakoutConfirmCfg(
        l2_stale_ms=1500,
        min_wall_notional=25_000.0,
        max_near_wall_bps=4.0,
    )
    v = L2ConfirmBreakout(cfg=cfg)

    lvl = 100.00
    # nearest bid wall very close: 3 bps away, notional big => veto
    l2 = _mk_l2(
        bids=[(99.97, 80_000.0), (99.90, 5_000.0)],
        asks=[(100.01, 10_000.0)],
    )
    ctx = SimpleNamespace(
        price=99.98,
        ts_ms=10_000,
        l2_ts_ms=9_999,
    )
    r = v.confirm(ctx=ctx, side="sell", level_price=lvl, l2=l2)
    assert r.veto is True
    assert r.reason_code == VETO_WALL_NEAR
    assert float(r.score01) == 0.0


def test_functional_breakout_no_longer_vetoes_wall_near_buy():
    """
    After unification: functional l2_confirm_breakout must NOT veto on wall_near.
    It may still emit near_wall feature in parts for downstream scoring/analytics.
    """
    lvl = 100.00
    l2 = _mk_l2(
        bids=[(99.99, 10_000.0)],
        asks=[(100.01, 100_000.0)],  # very close, big notional
    )
    ctx = SimpleNamespace(
        spread_bps=1.0,  # not veto by spread
        microprice_shift_bps_20=0.0,  # not veto by mp contra
    )

    res = l2_confirm_breakout(
        ctx=ctx,
        l2=l2,
        level_price=lvl,
        side="buy",
        max_spread_bps=8.0,
        wall_near_bps=6.0,          # near
        min_wall_notional=50_000.0, # qualifies as "big"
        mp_contra_bps=2.0,
    )

    # L2ConfirmResult is expected to expose these attributes (as in your current code).
    assert res.veto is False
    assert res.reason_code == OK
    parts = res.parts
    assert parts.get("near_wall") == 1
    assert parts.get("wall_dist_bps") is not None


def test_functional_breakout_still_vetoes_on_mp_contra():
    """
    Unification removes only wall_near veto from functional path.
    Other vetos (spread, mp_contra) remain intact and deterministic.
    """
    lvl = 100.00
    l2 = _mk_l2(
        bids=[(99.99, 10_000.0)],
        asks=[(100.50, 10_000.0)],
    )
    ctx = SimpleNamespace(
        spread_bps=1.0,
        microprice_shift_bps_20=-5.0,  # contra for buy
    )
    res = l2_confirm_breakout(
        ctx=ctx,
        l2=l2,
        level_price=lvl,
        side="buy",
        mp_contra_bps=2.0,
    )
    assert res.veto is True
