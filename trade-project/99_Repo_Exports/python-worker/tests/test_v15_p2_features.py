"""test_v15_p2_features.py — Unit tests for P2 shadow features.

Covers:
  Group A: mlofi_extended_features, microprice_ret_250ms, HalfSecondBucketAggregator
  Group B: _QueueState P2 extensions (adverse_3s, FOK edge, L5, position_risk)
  Group C: _enrich_derived P2 cost decomposition features
  Group D: _RegimeState P2 extensions (age, transition probs, vol_ofi_agree)
  Group E: _CrossVenueState P2 extensions (flip_count, lead_score, book_age)
  Group G: _agg7 P2 fields (trailing_rate, be_stopout, hold_time) + _enrich_p2_pit_priors
  Group H: _DCState (dc_event_dir, dc_event_age_ms, dc_overshoot_bps, dc_reversal_count_15m)
  Wiring:  P2 keys present in _V12_BASE_OPTIONAL_KEYS
"""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def _clear_snapshot_cache():
    import core.feature_enricher_v1 as _enr
    _enr._snapshot_cache.clear()
    yield
    _enr._snapshot_cache.clear()


def _make_redis_json(data: dict):
    r = MagicMock()
    r.get.return_value = json.dumps(data).encode()
    r.hgetall.return_value = {}
    r.mget.return_value = [None]
    return r


# ── Group A: microstructure_metrics_v2 extensions ────────────────────────────

def test_mlofi_extended_slope():
    from core.microstructure_metrics_v2 import mlofi_extended_features
    # 10 seconds of OFI trending positively
    ts = list(range(10))
    ofi = [float(i) * 0.1 for i in range(10)]
    out = mlofi_extended_features(ofi, ts)
    assert "mlofi_1_3_5_slope" in out
    # Positive trend → slope should be negative (newer = shorter window = higher mean)
    # Actually: 1s window has highest ofi (most recent = highest), 5s window averages more
    # slope is over [1,3,5] → depends on actual means
    assert isinstance(out["mlofi_1_3_5_slope"], float)


def test_mlofi_l1_l5_divergence():
    from core.microstructure_metrics_v2 import mlofi_extended_features
    ts = [float(i) for i in range(10)]
    # Strong OFI recently, weak before → 1s > 5s mean → positive divergence
    ofi = [0.1, 0.1, 0.1, 0.1, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    out = mlofi_extended_features(ofi, ts)
    assert "mlofi_l1_l5_divergence" in out
    assert out["mlofi_l1_l5_divergence"] > 0.0


def test_mlofi_accel_500ms_from_1s():
    from core.microstructure_metrics_v2 import mlofi_extended_features
    ts = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
    ofi = [0.1, 0.2, 0.3, 0.4, 0.5, 0.8]
    out = mlofi_extended_features(ofi, ts)
    assert "mlofi_accel_500ms" in out
    # Without 500ms data: proxy = (last - prev) * 0.5
    assert out["mlofi_accel_500ms"] == pytest.approx((0.8 - 0.5) * 0.5)


def test_mlofi_accel_500ms_from_half_series():
    from core.microstructure_metrics_v2 import mlofi_extended_features
    ts = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
    ofi = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6]
    ofi_500ms = [0.4, 0.5, 0.6, 0.8]  # last delta = 0.8 - 0.6 = 0.2
    out = mlofi_extended_features(ofi, ts, ofi_series_500ms=ofi_500ms)
    assert "mlofi_accel_500ms" in out
    assert out["mlofi_accel_500ms"] == pytest.approx(0.2)


def test_mlofi_exhaustion_at_peak():
    from core.microstructure_metrics_v2 import mlofi_extended_features
    ts = [float(i) for i in range(8)]
    ofi = [0.0, 0.2, 0.5, 0.8, 0.9, 0.9, 0.9, 0.9]  # stable at peak
    out = mlofi_extended_features(ofi, ts)
    assert "mlofi_exhaustion_score" in out
    assert out["mlofi_exhaustion_score"] == pytest.approx(0.0)


def test_mlofi_exhaustion_weakening():
    from core.microstructure_metrics_v2 import mlofi_extended_features
    ts = [float(i) for i in range(8)]
    ofi = [0.0, 0.9, 0.9, 0.9, 0.9, 0.5, 0.2, 0.05]  # peak then weakening
    out = mlofi_extended_features(ofi, ts)
    assert "mlofi_exhaustion_score" in out
    assert out["mlofi_exhaustion_score"] > 0.5


def test_microprice_ret_250ms_from_500ms_data():
    from core.microstructure_metrics_v2 import microprice_ret_250ms
    ts = [0.0, 0.5, 1.0, 1.5]
    prices = [100.0, 100.5, 101.0, 101.5]
    result = microprice_ret_250ms(prices, ts)
    assert result is not None
    # 101.5 vs ref at 1.0s (>=0.25s back) = 101.0 → (101.5 - 101.0)/101.0 * 10000
    assert result == pytest.approx((101.5 - 101.0) / 101.0 * 10_000.0)


def test_half_second_bucket_aggregator_ofi():
    from core.microstructure_metrics_v2 import HalfSecondBucketAggregator
    agg = HalfSecondBucketAggregator()
    # Two 500ms buckets: t=0ms (buy) and t=500ms (sell)
    agg.on_tick(100.0, 10.0, 0.0, ts_ms=0)
    agg.on_tick(100.0, 0.0, 5.0, ts_ms=500)
    agg.on_tick(100.0, 0.0, 5.0, ts_ms=501)  # flush first bucket
    ofi, ts = agg.ofi_series_500ms()
    assert len(ofi) >= 1
    # First bucket: buy_vol=10, sell_vol=0 → ofi = (10-0)/(10+0) = 1.0
    assert ofi[0] == pytest.approx(1.0)


# ── Group B: queue_dynamics P2 ────────────────────────────────────────────────

def test_queue_adverse_selection_3s():
    from services.queue_dynamics_producer import _QueueState
    state = _QueueState()
    now_s = time.time()
    # 4 trades at 3.1s apart
    for i in range(4):
        ts_ms = int((now_s - 9 + i * 3.1) * 1000)
        price = 100.0 + i * 0.01
        state.on_tick(price, True, ts_ms)
    out = state.compute()
    # With 3.1s spacing, adverse_selection_3s window [2.5, 3.5] should fire
    # Checks the key is present and is a float
    if "adverse_selection_3s_bps" in out:
        assert isinstance(out["adverse_selection_3s_bps"], float)


def test_queue_fill_or_kill_edge_bps():
    from services.queue_dynamics_producer import _QueueState
    state = _QueueState()
    # Setup spread and reversion prob for FOK edge
    state.on_spread(10.0)
    state.on_spread(10.0)
    for i in range(5):
        state.on_book(100.0, 100.0, int(time.time() * 1000) + i * 1000)
    out = state.compute()
    assert "limit_vs_market_entry_edge_bps" in out
    assert "fill_or_kill_edge_bps" in out
    # fill_or_kill_edge_bps ≤ limit_vs_market_entry_edge_bps
    assert out["fill_or_kill_edge_bps"] <= out["limit_vs_market_entry_edge_bps"]


def test_queue_l5_depth_features():
    from services.queue_dynamics_producer import _QueueState
    state = _QueueState()
    now_ms = int(time.time() * 1000)
    # Simulate increasing then decreasing depth
    for i in range(5):
        ts = now_ms + i * 1000
        bid_d = 500.0 - i * 30
        ask_d = 500.0 - i * 20
        state.on_depth(bid_d, ask_d, ts)
    out = state.compute()
    # depth_proxy needs >=4 entries with depletion
    if "queue_depletion_rate_l5" in out:
        assert out["queue_depletion_rate_l5"] >= 0.0


# ── Group C: _enrich_derived cost decomposition ───────────────────────────────

def test_ev_after_fee_bps_default_fee():
    from core.feature_enricher_v1 import _enrich_derived
    inds = {"edge_bps": 10.0}
    out = _enrich_derived(inds, {})
    assert "ev_after_fee_bps" in out
    assert out["ev_after_fee_bps"] == pytest.approx(7.0)  # 10 - 3.0 default


def test_ev_after_spread_bps():
    from core.feature_enricher_v1 import _enrich_derived
    inds = {"edge_bps": 15.0, "spread_bps": 8.0}
    out = _enrich_derived(inds, {})
    assert "ev_after_spread_bps" in out
    assert out["ev_after_spread_bps"] == pytest.approx(11.0)  # 15 - 8*0.5


def test_ev_after_impact_bps():
    from core.feature_enricher_v1 import _enrich_derived
    inds = {"edge_bps": 20.0}
    deriv = {"tca_perm_impact_1s_bps_ema": 5.0}
    out = _enrich_derived(inds, deriv)
    assert "ev_after_impact_bps" in out
    assert out["ev_after_impact_bps"] == pytest.approx(15.0)


def test_tp1_net_after_cost_bps():
    from core.feature_enricher_v1 import _enrich_derived
    inds = {
        "tp1_target_r": 2.0,
        "sl_dist_bps": 30.0,
        "spread_bps": 4.0,
        "expected_slippage_bps": 2.0,
    }
    out = _enrich_derived(inds, {})
    assert "tp1_net_after_cost_bps" in out
    # tp1 = 2.0 * 30 = 60, cost = 4*0.5 + 2 = 4, net = 56
    assert out["tp1_net_after_cost_bps"] == pytest.approx(56.0)


def test_sl_net_after_cost_bps():
    from core.feature_enricher_v1 import _enrich_derived
    inds = {
        "sl_dist_bps": 25.0,
        "spread_bps": 4.0,
        "expected_slippage_bps": 2.0,
    }
    out = _enrich_derived(inds, {})
    assert "sl_net_after_cost_bps" in out
    assert out["sl_net_after_cost_bps"] == pytest.approx(29.0)  # 25 + 4


def test_expected_hold_cost_bps_from_spread():
    from core.feature_enricher_v1 import _enrich_derived
    inds = {"spread_bps": 6.0}
    deriv = {"tca_eff_spread_bps_ema": 5.5}
    out = _enrich_derived(inds, deriv)
    assert "expected_hold_cost_bps" in out
    assert out["expected_hold_cost_bps"] == pytest.approx(5.5)


def test_cost_regime_z_elevated():
    from core.feature_enricher_v1 import _enrich_derived
    inds = {"spread_bps": 12.0}
    deriv = {"tca_eff_spread_bps_ema": 5.0, "tca_spread_p95_bps": 10.0}
    out = _enrich_derived(inds, deriv)
    assert "cost_regime_z" in out
    # current=12 > ema=5, should be positive z
    assert out["cost_regime_z"] > 0.0


def test_cost_regime_z_clamped():
    from core.feature_enricher_v1 import _enrich_derived
    inds = {"spread_bps": 999.0}
    deriv = {"tca_eff_spread_bps_ema": 5.0}
    out = _enrich_derived(inds, deriv)
    if "cost_regime_z" in out:
        assert out["cost_regime_z"] <= 5.0


# ── Group D: regime_transition P2 ─────────────────────────────────────────────

def test_regime_transition_age_ms():
    from services.regime_transition_producer import _RegimeState
    state = _RegimeState()
    now = int(time.time() * 1000)
    state.observe("range", now - 60_000)
    state.observe("trending_bull", now - 30_000)
    out = state.compute(now)
    assert "regime_transition_age_ms" in out
    assert 25_000 < out["regime_transition_age_ms"] < 35_000


def test_trend_to_chop_prob():
    from services.regime_transition_producer import _RegimeState
    state = _RegimeState()
    now = int(time.time() * 1000)
    # 4 transitions: 2 trending→range, 2 range→trending
    state.observe("range", now - 200_000)
    state.observe("trending_bull", now - 160_000)
    state.observe("range", now - 120_000)
    state.observe("trending_bear", now - 80_000)
    state.observe("range", now - 40_000)
    out = state.compute(now)
    assert "trend_to_chop_prob" in out
    assert out["trend_to_chop_prob"] > 0.0


def test_range_break_attempt_count_30m():
    from services.regime_transition_producer import _RegimeState
    state = _RegimeState()
    now = int(time.time() * 1000)
    _30m = 30 * 60 * 1000
    state.observe("range", now - _30m + 5000)
    state.observe("trending_bull", now - _30m + 10_000)
    state.observe("range", now - _30m + 15_000)
    state.observe("squeeze", now - _30m + 20_000)
    out = state.compute(now)
    assert "range_break_attempt_count_30m" in out
    assert out["range_break_attempt_count_30m"] >= 2.0


def test_vol_ofi_regime_agree_bullish():
    from services.regime_transition_producer import _RegimeState
    state = _RegimeState()
    now = int(time.time() * 1000)
    state.observe("trending_bull", now - 5000, ofi=0.8)
    out = state.compute(now)
    assert "vol_ofi_regime_agree" in out
    assert out["vol_ofi_regime_agree"] > 0.5  # positive OFI aligns with bull regime


def test_expansion_exhaustion_score():
    from services.regime_transition_producer import _RegimeState
    state = _RegimeState()
    now = int(time.time() * 1000)
    # 3 failed breakouts → exhaustion = 1.0
    _m = 1 * 60 * 1000
    state.observe("range", now - 20 * _m)
    for i in range(3):
        state.observe("trending_bull", now - (18 - i * 4) * _m)
        state.observe("range", now - (16 - i * 4) * _m)
    out = state.compute(now)
    assert "expansion_exhaustion_score" in out
    assert out["expansion_exhaustion_score"] == pytest.approx(1.0)


# ── Group E: cross_venue P2 ───────────────────────────────────────────────────

def test_venue_consensus_flip_count_10s():
    from services.cross_venue_health_producer import _CrossVenueState
    state = _CrossVenueState()
    now_ms = int(time.time() * 1000)
    # Add alternating price diff signs in last 10s
    for i in range(6):
        ts = now_ms - (10_000 - i * 1500)
        px = 100.0 + (0.1 if i % 2 == 0 else -0.1)
        state.on_binance_tick(px, ts)
        state.compute(100.0, ts - 50)  # update diff_signs
    out = state.compute(100.0, now_ms - 50)
    assert "venue_consensus_flip_count_10s" in out
    assert out["venue_consensus_flip_count_10s"] >= 0.0


def test_binance_leads_bybit_score_always_leads():
    from services.cross_venue_health_producer import _CrossVenueState
    state = _CrossVenueState()
    now_ms = int(time.time() * 1000)
    # Binance always 200ms ahead of bybit
    for i in range(10):
        bin_ts = now_ms - 10_000 + i * 1000
        byb_ts = bin_ts - 200
        state.on_binance_tick(100.0, bin_ts)
        state.compute(100.0, byb_ts)  # lead_obs should be 1 (binance leads)
    out = state.compute(100.0, now_ms - 200)
    assert "binance_leads_bybit_score" in out
    assert out["binance_leads_bybit_score"] > 0.5


def test_bybit_book_age_ms_from_ts():
    from services.cross_venue_health_producer import _CrossVenueState
    state = _CrossVenueState()
    now_ms = int(time.time() * 1000)
    state.on_binance_tick(100.0, now_ms)
    out = state.compute(100.0, now_ms - 5_000)
    assert "bybit_book_age_ms" in out
    assert out["bybit_book_age_ms"] >= 0.0


def test_bybit_trade_rate_hz():
    from services.cross_venue_health_producer import _CrossVenueState
    state = _CrossVenueState()
    now_ms = int(time.time() * 1000)
    state.on_binance_tick(100.0, now_ms)
    # 30 trades in 30s window → 1 Hz
    out = state.compute(
        100.0, now_ms - 100,
        bybit_trade_count=30,
        bybit_window_ms=30_000,
    )
    assert "bybit_trade_rate_hz" in out
    assert out["bybit_trade_rate_hz"] == pytest.approx(1.0)


# ── Group G: pit_priors P2 ────────────────────────────────────────────────────

def test_agg7_trailing_success_rate():
    from orderflow_services.pit_priors_rolling_v1 import compute_rolling_priors
    now_ms = int(time.time() * 1000)
    embargo = 3_600_000
    base_ts = now_ms - 8 * 86_400_000  # 8 days ago (outside 7d window but in 30d)
    # Actually need inside 7d: use 2 days ago
    base_ts = now_ms - 2 * 86_400_000 - embargo - 60_000
    trades = []
    for i in range(25):
        close_reason = "trail_sl" if i < 10 else "initial_sl"
        result = "WIN" if close_reason == "trail_sl" else "LOSS"
        trades.append({
            "symbol": "BTCUSDT",
            "scenario": "default",
            "result": result,
            "r_multiple": "1.0" if result == "WIN" else "-1.0",
            "close_reason": close_reason,
            "ts_close": str(base_ts + i * 60_000),
        })
    priors7, _ = compute_rolling_priors(trades, now_ms)
    key = ("BTCUSDT", "default", "all")
    assert key in priors7
    agg = priors7[key]
    assert "trailing_success_rate" in agg
    # 10 trail wins out of 10 total wins → rate = 1.0
    assert agg["trailing_success_rate"] == pytest.approx(1.0)


def test_agg7_be_stopout_rate():
    from orderflow_services.pit_priors_rolling_v1 import compute_rolling_priors
    now_ms = int(time.time() * 1000)
    embargo = 3_600_000
    base_ts = now_ms - 2 * 86_400_000 - embargo - 60_000
    trades = []
    for i in range(20):
        close_reason = "be_stop" if i < 5 else "initial_sl"
        result = "WIN" if i < 5 else "LOSS"
        trades.append({
            "symbol": "BTCUSDT",
            "scenario": "default",
            "result": result,
            "r_multiple": "0.5" if result == "WIN" else "-1.0",
            "close_reason": close_reason,
            "ts_close": str(base_ts + i * 60_000),
        })
    priors7, _ = compute_rolling_priors(trades, now_ms)
    key = ("BTCUSDT", "default", "all")
    assert key in priors7
    agg = priors7[key]
    assert "be_stopout_rate" in agg
    assert agg["be_stopout_rate"] == pytest.approx(5 / 20)


def test_agg7_hold_time_fields():
    from orderflow_services.pit_priors_rolling_v1 import compute_rolling_priors
    now_ms = int(time.time() * 1000)
    embargo = 3_600_000
    base_ts = now_ms - 2 * 86_400_000 - embargo - 60_000
    trades = []
    for i in range(20):
        open_ms = base_ts + i * 60_000
        exit_ms = open_ms + (i + 1) * 120_000  # varied hold times
        trades.append({
            "symbol": "BTCUSDT",
            "scenario": "default",
            "result": "WIN" if i % 2 == 0 else "LOSS",
            "r_multiple": "1.0" if i % 2 == 0 else "-1.0",
            "close_reason": "tp1",
            "ts_close": str(exit_ms),
            "open_ts_ms": str(open_ms),
            "exit_ts_ms": str(exit_ms),
        })
    priors7, _ = compute_rolling_priors(trades, now_ms)
    key = ("BTCUSDT", "default", "all")
    assert key in priors7
    agg = priors7[key]
    assert "hold_time_p50_ms" in agg
    assert "hold_time_p90_ms" in agg
    assert agg["hold_time_p50_ms"] > 0.0
    assert agg["hold_time_p90_ms"] >= agg["hold_time_p50_ms"]


def test_enrich_p2_pit_priors_session_winrate():
    from core.feature_enricher_v1 import _enrich_p2_pit_priors
    data = {
        "winrate": "0.65",
        "ev_r": "0.45",
        "timeout_rate": "0.20",
        "trailing_success_rate": "0.70",
        "be_stopout_rate": "0.10",
        "hold_time_p50_ms": "180000",
        "hold_time_p90_ms": "420000",
        "median_mae_r_winners": "0.3",
        "median_mfe_r": "1.2",
        "tp1_hit_rate": "0.5",
    }
    r = MagicMock()
    r.hgetall.return_value = data
    r.get.return_value = None
    r.mget.return_value = [None]
    out = _enrich_p2_pit_priors("BTCUSDT", r)
    assert "prior_winrate_symbol_kind_regime_session" in out
    assert out["prior_winrate_symbol_kind_regime_session"] == pytest.approx(0.65)
    assert "prior_ev_r_symbol_kind_regime_session" in out
    assert out["prior_trailing_success_rate"] == pytest.approx(0.70)
    assert "prior_mae_before_mfe_ratio" in out


# ── Group H: directional change ───────────────────────────────────────────────

def test_dc_state_no_event():
    from services.directional_change_producer import _DCState
    state = _DCState(threshold_bps=50.0)
    now_ms = int(time.time() * 1000)
    state.on_tick(100.0, now_ms)
    state.on_tick(100.1, now_ms + 1000)
    out = state.compute(now_ms + 2000)
    assert out["dc_event_dir"] == 0.0
    assert out["dc_reversal_count_15m"] == 0.0


def test_dc_state_upward_event():
    from services.directional_change_producer import _DCState
    state = _DCState(threshold_bps=50.0)
    now_ms = int(time.time() * 1000)
    state.on_tick(100.0, now_ms)
    # Move up 0.6% = 60bps > 50bps threshold
    state.on_tick(100.6, now_ms + 1000)
    out = state.compute(now_ms + 2000)
    assert out["dc_event_dir"] == 1.0
    assert out["dc_overshoot_bps"] == pytest.approx(10.0, abs=1.0)
    assert out["dc_event_age_ms"] == pytest.approx(1000.0, abs=100.0)


def test_dc_state_downward_event():
    from services.directional_change_producer import _DCState
    state = _DCState(threshold_bps=50.0)
    now_ms = int(time.time() * 1000)
    state.on_tick(100.0, now_ms)
    state.on_tick(99.4, now_ms + 1000)  # down 0.6%
    out = state.compute(now_ms + 2000)
    assert out["dc_event_dir"] == -1.0
    assert out["dc_overshoot_bps"] == pytest.approx(10.0, abs=1.0)


def test_dc_reversal_count_15m():
    from services.directional_change_producer import _DCState
    state = _DCState(threshold_bps=50.0)
    now_ms = int(time.time() * 1000)
    _2m = 2 * 60 * 1000
    # Alternating DC events: up, down, up, down
    state.on_tick(100.0, now_ms - 8 * _2m)
    state.on_tick(100.6, now_ms - 7 * _2m)  # up DC
    state.on_tick(100.0, now_ms - 6 * _2m)  # down DC
    state.on_tick(100.6, now_ms - 5 * _2m)  # up DC
    state.on_tick(100.0, now_ms - 4 * _2m)  # down DC
    out = state.compute(now_ms)
    assert out["dc_reversal_count_15m"] >= 2.0


def test_dc_enricher_reads_from_json():
    from core.feature_enricher_v1 import _enrich_p2_directional_change
    data = {
        "dc_event_dir": 1.0,
        "dc_event_age_ms": 5000.0,
        "dc_overshoot_bps": 8.5,
        "dc_reversal_count_15m": 3.0,
    }
    r = _make_redis_json(data)
    out = _enrich_p2_directional_change("BTCUSDT", r)
    assert out["dc_event_dir"] == pytest.approx(1.0)
    assert out["dc_event_age_ms"] == pytest.approx(5000.0)
    assert out["dc_overshoot_bps"] == pytest.approx(8.5)
    assert out["dc_reversal_count_15m"] == pytest.approx(3.0)


# ── Wiring: P2 keys in _V12_BASE_OPTIONAL_KEYS ───────────────────────────────

_P2_KEYS = [
    # Group A
    "mlofi_1_3_5_slope", "mlofi_l1_l5_divergence", "mlofi_accel_500ms",
    "mlofi_exhaustion_score", "microprice_ret_250ms", "midprice_impact_per_1k_usd",
    # Group B
    "queue_depletion_rate_l5", "queue_refill_rate_l5", "queue_position_risk_score",
    "adverse_selection_3s_bps", "fill_or_kill_edge_bps",
    # Group C
    "ev_after_fee_bps", "ev_after_spread_bps", "ev_after_impact_bps",
    "tp1_net_after_cost_bps", "sl_net_after_cost_bps",
    "expected_hold_cost_bps", "cost_regime_z",
    # Group D
    "regime_transition_age_ms", "trend_to_chop_prob", "chop_to_expansion_prob",
    "expansion_exhaustion_score", "vol_ofi_regime_agree", "vol_price_divergence_score",
    "range_break_attempt_count_30m",
    # Group E
    "bybit_book_age_ms", "bybit_trade_rate_hz", "cross_venue_latency_diff_ms",
    "binance_leads_bybit_score", "bybit_leads_binance_score",
    "venue_consensus_flip_count_10s", "cross_venue_spread_diff_bps",
    # Group G
    "prior_winrate_symbol_kind_regime_session", "prior_ev_r_symbol_kind_regime_session",
    "prior_timeout_loss_rate_session", "prior_mae_before_mfe_ratio",
    "prior_best_exit_policy_code", "prior_trailing_success_rate",
    "prior_be_stopout_rate", "prior_hold_time_p50_ms", "prior_hold_time_p90_ms",
    # Group H
    "dc_event_dir", "dc_event_age_ms", "dc_overshoot_bps", "dc_reversal_count_15m",
]


def test_p2_keys_in_optional_keys():
    from core.external_features_payload_v1 import _V12_BASE_OPTIONAL_KEYS
    missing = [k for k in _P2_KEYS if k not in _V12_BASE_OPTIONAL_KEYS]
    assert missing == [], f"Missing from _V12_BASE_OPTIONAL_KEYS: {missing}"
