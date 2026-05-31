from types import SimpleNamespace

from contexts import L2Level, LiquidityPattern, SimpleL2Snapshot
from handlers.geometry_service import GeometryLiquidityService


def _cfg(**kwargs):
    # config.liquidity.<...> used by _detect_liquidity_pattern
    liquidity = SimpleNamespace(
        min_aggr_to_rest_ratio=kwargs.get("min_aggr_to_rest_ratio", 0.1),
        min_side_domination_ratio=kwargs.get("min_side_domination_ratio", 1.5),
    )
    geometry = SimpleNamespace(
        near_mult=kwargs.get("near_mult", 0.25),
        far_mult=kwargs.get("far_mult", 1.0),
    )
    return SimpleNamespace(liquidity=liquidity, geometry=geometry)


def _l2(bids, asks, mid=0.0, best_bid=0.0, best_ask=0.0):
    return SimpleL2Snapshot(
        bids=bids,
        asks=asks,
        ts_ms=0,
        mid=mid,
        best_bid=best_bid,
        best_ask=best_ask,
        depth_bid_5=0.0,
        depth_ask_5=0.0,
        depth_bid_20=0.0,
        depth_ask_20=0.0,
    )


def test_attach_liquidity_context_keyword_does_not_crash():
    svc = GeometryLiquidityService("BTCUSDT", _cfg())
    ctx = SimpleNamespace()  # method only writes attributes

    bids = [L2Level(price=100.0, size=10.0)]
    asks = [L2Level(price=101.0, size=10.0)]
    l2 = _l2(bids, asks, mid=100.5, best_bid=100.0, best_ask=101.0)

    # Before fix it raised TypeError because of wrong kw: cluster=...
    svc._attach_liquidity_context(ctx, l2=l2, cluster_vol=None)

    assert hasattr(ctx, "liquidity_context")
    assert ctx.liquidity_context is not None


def test_find_near_liquidity_wall_no_mid_no_div0():
    svc = GeometryLiquidityService("BTCUSDT", _cfg())

    bids = [L2Level(price=100.0, size=100.0)]
    asks = [L2Level(price=101.0, size=100.0)]

    # mid=0 and best_bid/best_ask=0 -> must not crash, must return no wall
    l2 = _l2(bids, asks, mid=0.0, best_bid=0.0, best_ask=0.0)
    side, lvl, dist, z = svc._find_near_liquidity_wall(l2)

    assert side is None
    assert lvl is None
    assert dist is None
    assert z is None


def test_detect_liquidity_pattern_returns_enum_and_score_uses_enum():
    cfg = _cfg(min_aggr_to_rest_ratio=0.05, min_side_domination_ratio=1.5)
    svc = GeometryLiquidityService("BTCUSDT", cfg)

    # Strong buy dominance
    pat = svc._detect_liquidity_pattern(
        aggr_buy_at_wall=150.0,
        aggr_sell_at_wall=50.0,
        aggr_to_rest_ratio=0.2,
    )
    assert pat == LiquidityPattern.BUY_AGGR_CLUSTER

    lc = SimpleNamespace(pattern=pat)
    score = svc._score_liquidity(lc)  # should not depend on strings
    assert score > 0.10


def test_detect_liquidity_pattern_none_when_ratio_low():
    cfg = _cfg(min_aggr_to_rest_ratio=0.2)
    svc = GeometryLiquidityService("BTCUSDT", cfg)

    pat = svc._detect_liquidity_pattern(
        aggr_buy_at_wall=100.0,
        aggr_sell_at_wall=10.0,
        aggr_to_rest_ratio=0.05,  # below threshold
    )
    assert pat == LiquidityPattern.NONE
