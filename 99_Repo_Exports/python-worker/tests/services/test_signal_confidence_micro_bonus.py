from types import SimpleNamespace

from services.signal_confidence import ConfidenceConfig, ConfidenceScorer


def _mk_ctx(confirmations):
    return SimpleNamespace(
        z_delta=3.5,
        delta=10.0,
        obi_avg=0.3,
        obi_sustained=True,
        obi_avg_20=0.3,
        obi_sustained_20=True,
        microprice_shift_bps_20=0.0,
        wall_bid=False, wall_ask=False,
        wall_bid_dist_bps=1e9, wall_ask_dist_bps=1e9,
        depletion_score=0.0, refill_score=0.0,
        impact_proxy=0.0,
        spread_bps=5.0,
        realized_ema_bps=0.0,
        adverse_ratio_ema=0.0,
        market_mode="mixed",
        l2_age_ms=0.0, l2_is_stale=False,
        taker_buy_rate_ema=0.0, taker_sell_rate_ema=0.0,
        cancel_to_trade_ask=0.0, cancel_to_trade_bid=0.0,
        eta_fill_ask_sec=0.0, eta_fill_bid_sec=0.0,
        weak_progress=False,
        confirmations=confirmations,
        # micro bonus config
        micro_bonus_cap=0.10,
        obi_stable_min_secs=1.5,
        obi_stable_bonus_w=0.05,
        ofi_min_secs=1.0,
        ofi_bonus_w=0.03,
        cvd_reclaim_bonus_w=0.02,
    )


async def test_micro_bonus_increases_confidence():
    s = ConfidenceScorer(cfg=ConfidenceConfig())
    base_ctx = _mk_ctx(confirmations=[])
    c0, _ = await s.score(kind="custom", side="LONG", ctx=base_ctx)

    bonus_ctx = _mk_ctx(confirmations=["obi_stable=2.00", "obi_q=0.90"])
    c1, _ = await s.score(kind="custom", side="LONG", ctx=bonus_ctx)
    assert c1 >= c0
