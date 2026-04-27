from __future__ import annotations

from types import SimpleNamespace


def _import_processor():
    # Support both package and flat execution layouts
    try:
        from handlers.data_processor import OrderFlowDataProcessor  # type: ignore
        return OrderFlowDataProcessor
    except Exception:
        from data_processor import OrderFlowDataProcessor  # type: ignore
        return OrderFlowDataProcessor


OrderFlowDataProcessor = _import_processor()


def cfg(**kw):
    d = dict(
        delta_window_ticks=100,
        l2_stale_ms=2000,
        spread_bps_max=15.0,
        wall_filter_persist_min=0.7,
        wall_filter_dist_max_bps=4.0,
        family="crypto_orderflow",
        venue="binance_futures",
        timeframe_s=60,
    )
    d.update(kw)
    return SimpleNamespace(**d)


def test_quality_gate_rejects_stale_l2():
    dp = OrderFlowDataProcessor(symbol="TEST", specs=None, config=cfg())
    ctx = SimpleNamespace(l2_is_stale=True)
    assert dp._exec_quality_ok(ctx, "buy") is False
    assert dp._exec_quality_ok(ctx, "sell") is False


def test_quality_gate_rejects_contradiction_buy_microprice_shift_negative():
    dp = OrderFlowDataProcessor(symbol="TEST", specs=None, config=cfg())

    # Pass OBI threshold but fail microprice sign
    ctx = SimpleNamespace(
        l2_is_stale=False,
        obi_20_valid=True,
        obi_sustained_20=True,
        obi_sustained=True,
        obi_avg_20=0.30,                 # >= thr (default 0.20)
        microprice_shift_bps_20=-0.10,   # contradiction for BUY
        # walls neutral
        wall_ask_persist_ratio=0.0,
        wall_ask_suspicious=False,
        wall_ask_dist_bps=1e9,
        wall_bid_persist_ratio=0.0,
        wall_bid_suspicious=False,
        wall_bid_dist_bps=1e9,
    )
    assert dp._exec_quality_ok(ctx, "buy") is False


def test_quality_gate_rejects_contradiction_sell_microprice_shift_positive():
    dp = OrderFlowDataProcessor(symbol="TEST", specs=None, config=cfg())

    ctx = SimpleNamespace(
        l2_is_stale=False,
        obi_20_valid=True,
        obi_sustained_20=True,
        obi_sustained=True,
        obi_avg_20=-0.30,                # <= -thr for SELL
        microprice_shift_bps_20=+0.10,   # contradiction for SELL
        # walls neutral
        wall_ask_persist_ratio=0.0,
        wall_ask_suspicious=False,
        wall_ask_dist_bps=1e9,
        wall_bid_persist_ratio=0.0,
        wall_bid_suspicious=False,
        wall_bid_dist_bps=1e9,
    )
    assert dp._exec_quality_ok(ctx, "sell") is False
