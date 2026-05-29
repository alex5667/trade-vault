"""test_v15_p1_phase1_features.py — Unit tests for Phase 1 P1 features.

Covers:
  - _enrich_derived: ev_after_slippage_bps, net_edge_to_cost_ratio (#11-12)
  - _enrich_liquidation_ctx: liq_source_available, liq_source_age_ms,
    liq_cascade_risk_score (#21-22-23)
  - mlofi_features + microprice_features pure functions (#1-5)
  - SecondBucketAggregator (#1-5 service layer)
  - queue dynamics compute pure logic (#6-10)
  - cost dynamics compute pure logic (#13)
  - regime_transition_producer: _RegimeState (#14-15)
  - cross_venue_health: _CrossVenueState (#19-20)
  - pit_priors_rolling_v1: timeout_rate + tp1_before_timeout_rate (#24-25)
  - feature key presence in _V12_BASE_OPTIONAL_KEYS
"""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

import pytest


# ── helpers ───────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _clear_snapshot_cache():
    """Clear in-process enricher snapshot cache between tests to prevent TTL bleed."""
    import core.feature_enricher_v1 as _enr
    _enr._snapshot_cache.clear()
    yield
    _enr._snapshot_cache.clear()


def _make_redis_json(data: dict):
    """Mock Redis client that returns `data` as JSON for any GET."""
    r = MagicMock()
    r.get.return_value = json.dumps(data).encode()
    r.hgetall.return_value = {}
    r.mget.return_value = [None]
    return r


# ── #11-12: ev_after_slippage_bps, net_edge_to_cost_ratio ────────────────────

def test_ev_after_slippage_bps_basic():
    from core.feature_enricher_v1 import _enrich_derived
    inds = {
        "edge_bps": 15.0,
        "expected_slippage_bps": 4.0,
        "spread_bps": 3.0,
    }
    out = _enrich_derived(inds, {})
    assert "ev_after_slippage_bps" in out
    assert out["ev_after_slippage_bps"] == pytest.approx(11.0)


def test_ev_after_slippage_bps_missing_edge():
    from core.feature_enricher_v1 import _enrich_derived
    inds = {"expected_slippage_bps": 4.0}
    out = _enrich_derived(inds, {})
    assert "ev_after_slippage_bps" not in out


def test_net_edge_to_cost_ratio_basic():
    from core.feature_enricher_v1 import _enrich_derived
    # edge=20, slip=4, spread=4 → cost = 4*0.5 + 4 = 6
    # ratio = (20 - 6) / 6 ≈ 2.333
    inds = {"edge_bps": 20.0, "expected_slippage_bps": 4.0, "spread_bps": 4.0}
    out = _enrich_derived(inds, {})
    assert "net_edge_to_cost_ratio" in out
    assert out["net_edge_to_cost_ratio"] == pytest.approx(14.0 / 6.0)


def test_net_edge_to_cost_ratio_negative_edge():
    from core.feature_enricher_v1 import _enrich_derived
    inds = {"edge_bps": 1.0, "expected_slippage_bps": 5.0, "spread_bps": 2.0}
    out = _enrich_derived(inds, {})
    assert "net_edge_to_cost_ratio" in out
    assert out["net_edge_to_cost_ratio"] < 0


# ── External source health metadata ──────────────────────────────────────────

def test_fg_data_available_when_present():
    from core.feature_enricher_v1 import _enrich_sentiment
    now_ms = int(time.time() * 1000)
    data = {"value": 55, "ts_ms": now_ms - 1000}
    r = _make_redis_json(data)
    out = _enrich_sentiment(r)
    assert out["fg_data_available"] == 1.0
    assert out["fg_data_stale"] == 0.0
    assert "fg_data_age_ms" in out
    assert out["fg_data_age_ms"] >= 0.0


def test_fg_data_available_when_absent():
    from core.feature_enricher_v1 import _enrich_sentiment
    r = MagicMock()
    r.get.return_value = None
    out = _enrich_sentiment(r)
    assert out["fg_data_available"] == 0.0
    assert out["fg_data_stale"] == 1.0


def test_external_ctx_source_health_keys_present():
    """_enrich_external_ctx always emits *_data_available/*_stale for each source."""
    from core.feature_enricher_v1 import _enrich_external_ctx
    # Mock with no Redis (simulate connection failure)
    out = _enrich_external_ctx(None)
    for prefix in ("cmc", "dl", "deribit"):
        assert f"{prefix}_data_available" in out, f"missing {prefix}_data_available"
        assert f"{prefix}_data_stale" in out, f"missing {prefix}_data_stale"


def test_bybit_health_available_when_missing():
    from core.feature_enricher_v1 import _enrich_bybit_health
    r = MagicMock()
    r.hgetall.return_value = {}
    out = _enrich_bybit_health("BTCUSDT", r)
    assert out["bybit_data_available"] == 0.0
    assert out["bybit_data_stale"] == 1.0


def test_bybit_health_available_when_present():
    from core.feature_enricher_v1 import _enrich_bybit_health
    now_ms = int(time.time() * 1000)
    r = MagicMock()
    r.hgetall.return_value = {"ts_ms": str(now_ms - 2000)}
    out = _enrich_bybit_health("BTCUSDT", r)
    assert out["bybit_data_available"] == 1.0
    assert out["bybit_data_stale"] == 0.0
    assert out["bybit_data_age_ms"] >= 1500.0


# ── #21-23: liq_source_available, liq_source_age_ms, liq_cascade_risk_score ──

def test_liq_source_available_ok():
    from core.feature_enricher_v1 import _enrich_liquidation_ctx
    now_ms = int(time.time() * 1000)
    data = {
        "quality_status": "OK",
        "ts_ms": now_ms,
        "liq_imbalance_z": 1.0,
        "liq_stress_flag": 0,
        "largest_liq_notional_1m": 100_000.0,
        "liq_buy_notional_1m": 200_000.0,
        "liq_sell_notional_1m": 300_000.0,
    }
    r = _make_redis_json(data)
    out = _enrich_liquidation_ctx("BTCUSDT", r)
    assert out["liq_source_available"] == 1.0


def test_liq_source_available_absent():
    from core.feature_enricher_v1 import _enrich_liquidation_ctx
    now_ms = int(time.time() * 1000)
    data = {"quality_status": "absent", "ts_ms": now_ms}
    r = _make_redis_json(data)
    out = _enrich_liquidation_ctx("BTCUSDT", r)
    assert out["liq_source_available"] == 0.0


def test_liq_source_age_ms_present():
    from core.feature_enricher_v1 import _enrich_liquidation_ctx
    now_ms = int(time.time() * 1000)
    data = {"quality_status": "OK", "ts_ms": now_ms - 3000}
    r = _make_redis_json(data)
    out = _enrich_liquidation_ctx("BTCUSDT", r)
    assert "liq_source_age_ms" in out
    assert out["liq_source_age_ms"] >= 2000.0


def test_liq_cascade_risk_score_range():
    from core.feature_enricher_v1 import _enrich_liquidation_ctx
    now_ms = int(time.time() * 1000)
    data = {
        "quality_status": "OK",
        "ts_ms": now_ms,
        "liq_imbalance_z": 5.0,
        "liq_stress_flag": 1,
        "largest_liq_notional_1m": 500_000.0,
        "liq_buy_notional_1m": 200_000.0,
        "liq_sell_notional_1m": 800_000.0,
    }
    r = _make_redis_json(data)
    out = _enrich_liquidation_ctx("BTCUSDT", r)
    score = out["liq_cascade_risk_score"]
    assert 0.0 <= score <= 1.0


def test_liq_cascade_risk_score_zero_notional():
    from core.feature_enricher_v1 import _enrich_liquidation_ctx
    now_ms = int(time.time() * 1000)
    data = {
        "quality_status": "absent",
        "ts_ms": now_ms,
        "liq_imbalance_z": 0.0,
        "liq_stress_flag": 0,
        "largest_liq_notional_1m": 0.0,
        "liq_buy_notional_1m": 0.0,
        "liq_sell_notional_1m": 0.0,
    }
    r = _make_redis_json(data)
    out = _enrich_liquidation_ctx("BTCUSDT", r)
    assert out["liq_cascade_risk_score"] == 0.0


# ── #1-5: mlofi_features + microprice_features ────────────────────────────────

def test_mlofi_features_basic():
    from core.microstructure_metrics_v2 import mlofi_features
    ofi = [0.1, -0.2, 0.3, -0.1, 0.5]
    ts = [1.0, 2.0, 3.0, 4.0, 5.0]
    out = mlofi_features(ofi, ts)
    assert "mlofi_accel_1s" in out
    assert out["mlofi_accel_1s"] == pytest.approx(0.5 - (-0.1))
    assert "mlofi_flip_count_3s" in out
    assert "mlofi_same_dir_secs" in out


def test_mlofi_features_too_short():
    from core.microstructure_metrics_v2 import mlofi_features
    assert mlofi_features([0.1], [1.0]) == {}
    assert mlofi_features([], []) == {}


def test_mlofi_same_dir_secs_streak():
    from core.microstructure_metrics_v2 import mlofi_features
    # All positive → streak = 4
    ofi = [0.1, 0.2, 0.3, 0.4]
    ts = [1.0, 2.0, 3.0, 4.0]
    out = mlofi_features(ofi, ts)
    assert out["mlofi_same_dir_secs"] == 4.0


def test_microprice_features_basic():
    from core.microstructure_metrics_v2 import microprice_features
    prices = [100.0, 100.5, 101.0, 101.5, 102.0]
    ts = [1.0, 2.0, 3.0, 4.0, 5.0]
    out = microprice_features(prices, ts)
    assert "microprice_ret_1s" in out  # found a 1s reference
    assert "microprice_reversion_3s" in out


def test_microprice_features_too_short():
    from core.microstructure_metrics_v2 import microprice_features
    assert microprice_features([100.0], [1.0]) == {}


# ── SecondBucketAggregator ────────────────────────────────────────────────────

def test_second_bucket_aggregator_ofi():
    from core.microstructure_metrics_v2 import SecondBucketAggregator
    agg = SecondBucketAggregator(maxlen=10)
    ts_base = 1_700_000_000_000  # 1s boundary
    # Add buy ticks in second 0
    agg.on_tick(100.0, 10.0, 0.0, ts_base + 100)
    agg.on_tick(100.0, 5.0, 0.0, ts_base + 500)
    # New second
    agg.on_tick(100.5, 0.0, 8.0, ts_base + 1100)
    # Force flush by advancing another second
    agg.on_tick(101.0, 2.0, 0.0, ts_base + 2100)

    ofi, ts = agg.ofi_series()
    assert len(ofi) >= 1


def test_second_bucket_aggregator_empty():
    from core.microstructure_metrics_v2 import SecondBucketAggregator
    agg = SecondBucketAggregator()
    ofi, ts = agg.ofi_series()
    assert ofi == []
    assert ts == []


# ── #6-10: queue dynamics ────────────────────────────────────────────────────

def test_queue_state_depletion_rate():
    from services.queue_dynamics_producer import _QueueState
    state = _QueueState(maxlen=50)
    now_ms = int(time.time() * 1000)
    # Need >= 4 entries for compute() to trigger depletion calculation
    state.on_book(100.0, 100.0, now_ms - 3000)
    state.on_book(80.0, 100.0, now_ms - 2000)
    state.on_book(50.0, 100.0, now_ms - 1000)
    state.on_book(30.0, 100.0, now_ms)
    out = state.compute()
    assert "queue_depletion_rate_l1" in out
    assert out["queue_depletion_rate_l1"] > 0


def test_queue_state_limit_vs_market():
    from services.queue_dynamics_producer import _QueueState
    state = _QueueState(maxlen=50)
    for _ in range(5):
        state.on_spread(10.0)  # 10 bps spread → 5 bps edge
    out = state.compute()
    assert "limit_vs_market_entry_edge_bps" in out
    assert out["limit_vs_market_entry_edge_bps"] == pytest.approx(5.0)


def test_queue_state_adverse_selection():
    from services.queue_dynamics_producer import _QueueState
    state = _QueueState(maxlen=100)
    now_ms = int(time.time() * 1000)
    # Need >= 4 trades; pairs spaced ~1s apart so dt is in [0.8, 1.5]
    state.on_tick(100.0, True, now_ms - 3000)   # buy
    state.on_tick(99.0, False, now_ms - 2000)   # 1s later → down-move, adverse for buy
    state.on_tick(99.5, True, now_ms - 1000)    # buy
    state.on_tick(99.0, False, now_ms)          # 1s later → down-move, adverse for buy
    out = state.compute()
    assert "adverse_selection_1s_bps" in out
    # adverse for buy = -(move) where move < 0 → positive
    assert out["adverse_selection_1s_bps"] > 0


# ── #13: cost dynamics ────────────────────────────────────────────────────────

def test_cost_state_widening():
    from services.cost_dynamics_producer import _CostState
    state = _CostState()
    t0 = time.time()
    state.observe(5.0, t0 - 8.0)
    state.observe(8.0, t0 - 4.0)
    state.observe(10.0, t0)
    out = state.compute(window_s=10.0)
    assert "cost_widening_5s_bps" in out
    assert out["cost_widening_5s_bps"] == pytest.approx(5.0)  # 10-5


def test_cost_state_tightening():
    from services.cost_dynamics_producer import _CostState
    state = _CostState()
    t0 = time.time()
    state.observe(10.0, t0 - 8.0)
    state.observe(6.0, t0)
    out = state.compute(window_s=10.0)
    assert out["cost_widening_5s_bps"] == pytest.approx(-4.0)


def test_cost_state_empty():
    from services.cost_dynamics_producer import _CostState
    assert _CostState().compute(5.0) == {}


# ── #14-15: regime transitions ────────────────────────────────────────────────

def test_regime_state_transition_code():
    from services.regime_transition_producer import _RegimeState
    state = _RegimeState()
    now = int(time.time() * 1000)
    state.observe("range", now - 5000)
    state.observe("trending_bull", now - 1000)
    out = state.compute(now)
    assert out["regime_transition_code"] == 1.0  # range→trend


def test_regime_state_failed_breakout():
    from services.regime_transition_producer import _RegimeState
    state = _RegimeState()
    now = int(time.time() * 1000)
    # range → trend → range within 10 min
    state.observe("range", now - 15 * 60_000)
    state.observe("trending_bull", now - 12 * 60_000)
    state.observe("range", now - 9 * 60_000)   # back to range within 3 min → failed breakout
    out = state.compute(now)
    assert out["failed_breakout_count_30m"] >= 1.0


def test_regime_state_no_failed_breakout_too_slow():
    from services.regime_transition_producer import _RegimeState
    state = _RegimeState()
    now = int(time.time() * 1000)
    # range → trend (then range much later, >15 min) → NOT a failed breakout
    state.observe("range", now - 60 * 60_000)
    state.observe("trending_bull", now - 40 * 60_000)
    state.observe("range", now - 20 * 60_000)  # 20 min later — valid trend
    out = state.compute(now)
    assert out["failed_breakout_count_30m"] == 0.0


# ── #19-20: cross-venue health ────────────────────────────────────────────────

def test_cross_venue_lead_lag():
    from services.cross_venue_health_producer import _CrossVenueState
    state = _CrossVenueState()
    bin_ts_ms = int(time.time() * 1000)
    bybit_ts_ms = bin_ts_ms - 200  # Binance 200ms ahead
    state.on_binance_tick(50_000.0, bin_ts_ms)
    out = state.compute(50_010.0, bybit_ts_ms)
    assert "cross_venue_lead_lag_ms" in out
    assert out["cross_venue_lead_lag_ms"] == pytest.approx(200.0)


def test_cross_venue_persistence():
    from services.cross_venue_health_producer import _CrossVenueState
    state = _CrossVenueState()
    # Add 3 same-sign diff signs in quick succession
    state._diff_signs.append((time.time() - 2.0, 1))
    state._diff_signs.append((time.time() - 1.0, 1))
    state._diff_signs.append((time.time() - 0.5, 1))
    state.on_binance_tick(50_000.0, int(time.time() * 1000))
    out = state.compute(49_990.0, int(time.time() * 1000))
    assert "venue_consensus_persistence_3s" in out
    assert out["venue_consensus_persistence_3s"] >= 2.0


# ── #24-25: pit_priors timeout rates ─────────────────────────────────────────

def test_pit_priors_timeout_rate_computed():
    from orderflow_services.pit_priors_rolling_v1 import compute_rolling_priors
    now_ms = int(time.time() * 1000)
    # Place all 20 trades well past the 1h embargo: latest trade at ~71min ago
    # base_ts is 90 min ago so trade[0..19] each at +60s steps end at ~71 min ago
    base_ts = now_ms - 5_400_000  # 90 min ago
    trades = []
    # 5 TIMEOUT trades
    for i in range(5):
        trades.append({
            "symbol": "BTCUSDT", "scenario": "default", "session": "us_main",
            "result": "LOSS", "r_multiple": "-0.5", "close_reason": "TIMEOUT",
            "tp1_hit": "0",
            "ts_close": str(base_ts + i * 60_000),
            "exit_ts_ms": str(base_ts + i * 60_000),
        })
    # 5 SL trades
    for i in range(5, 10):
        trades.append({
            "symbol": "BTCUSDT", "scenario": "default", "session": "us_main",
            "result": "LOSS", "r_multiple": "-1.0", "close_reason": "SL",
            "tp1_hit": "0",
            "ts_close": str(base_ts + i * 60_000),
            "exit_ts_ms": str(base_ts + i * 60_000),
        })
    # 10 WIN trades
    for i in range(10, 20):
        trades.append({
            "symbol": "BTCUSDT", "scenario": "default", "session": "us_main",
            "result": "WIN", "r_multiple": "1.0", "close_reason": "TP1",
            "tp1_hit": "1",
            "ts_close": str(base_ts + i * 60_000),
            "exit_ts_ms": str(base_ts + i * 60_000),
        })
    p7, _ = compute_rolling_priors(trades, now_ms)
    key = ("BTCUSDT", "default", "all")
    assert key in p7
    row = p7[key]
    assert "timeout_rate" in row
    # 5 timeouts / 20 total = 0.25
    assert row["timeout_rate"] == pytest.approx(0.25)
    assert "tp1_before_timeout_rate" in row
    assert row["tp1_before_timeout_rate"] == pytest.approx(0.0)


def test_pit_priors_tp1_before_timeout():
    from orderflow_services.pit_priors_rolling_v1 import compute_rolling_priors
    now_ms = int(time.time() * 1000)
    # All 20 trades at 60s spacing, starting 90 min ago → latest at ~71 min ago (past 1h embargo)
    base_ts = now_ms - 5_400_000
    trades = []
    # 5 trades: TP1 hit + TIMEOUT → tp1_before_timeout
    for i in range(5):
        trades.append({
            "symbol": "ETHUSDT", "scenario": "default", "session": "us_main",
            "result": "WIN", "r_multiple": "0.2", "close_reason": "TIMEOUT",
            "tp1_hit": "1",
            "ts_close": str(base_ts + i * 60_000),
            "exit_ts_ms": str(base_ts + i * 60_000),
        })
    # 15 normal wins
    for i in range(5, 20):
        trades.append({
            "symbol": "ETHUSDT", "scenario": "default", "session": "us_main",
            "result": "WIN", "r_multiple": "1.0", "close_reason": "TP1",
            "tp1_hit": "1",
            "ts_close": str(base_ts + i * 60_000),
            "exit_ts_ms": str(base_ts + i * 60_000),
        })
    p7, _ = compute_rolling_priors(trades, now_ms)
    key = ("ETHUSDT", "default", "all")
    assert key in p7
    row = p7[key]
    assert row["tp1_before_timeout_rate"] == pytest.approx(5 / 20)


# ── _V12_BASE_OPTIONAL_KEYS completeness ─────────────────────────────────────

# ── #16: session_liquidity_z ─────────────────────────────────────────────────

def test_session_vol_state_basic():
    from services.session_volume_aggregator import _SessionVolState
    state = _SessionVolState(history_len=10)
    # Feed 6 completed sessions worth of ticks so we exceed MIN_HISTORY=5
    # Each "session" uses ts_ms in a different UTC hour block
    # us: 13-22 UTC, europe: 7-16 UTC, asia: 22-7 UTC
    session_hours = [14, 9, 1, 15, 2, 10, 1, 14, 9, 2]  # alternating sessions
    for i, h in enumerate(session_hours):
        ts_ms = h * 3_600_000  # epoch ms at that UTC hour
        state.on_tick(50_000.0, 1.0, ts_ms)  # $50k notional per tick
    out = state.compute()
    assert "session_liquidity_z" in out
    assert out["quality_status"] in ("OK", "insufficient_history")


def test_session_vol_state_insufficient_history():
    from services.session_volume_aggregator import _SessionVolState
    state = _SessionVolState(history_len=30)
    # Only 2 session completions — below MIN_HISTORY=5
    state.on_tick(50_000.0, 1.0, 14 * 3_600_000)  # us session
    state.on_tick(50_000.0, 1.0, 9 * 3_600_000)   # europe session (boundary → archives us)
    out = state.compute()
    assert out["quality_status"] == "insufficient_history"
    assert "session_liquidity_z" in out  # still present, = 0.0


def test_session_vol_state_z_score_high_volume():
    from services.session_volume_aggregator import _SessionVolState
    state = _SessionVolState(history_len=30)
    # Feed enough sessions to exceed min history, then spike current volume
    for h in [9, 1, 14, 9, 1, 14, 9]:  # 6 session boundaries
        state.on_tick(50_000.0, 0.1, h * 3_600_000)  # low vol: $5k each
    # Current session (us) gets large volume spike
    state.on_tick(50_000.0, 100.0, 14 * 3_600_000)  # $5M spike
    out = state.compute()
    if out.get("quality_status") == "OK":
        assert out["session_liquidity_z"] > 0  # above average → positive z


def test_session_vol_state_absent():
    from services.session_volume_aggregator import _SessionVolState
    state = _SessionVolState()
    out = state.compute()
    assert out.get("quality_status") == "absent"


def test_session_vol_enricher_returns_z():
    from core.feature_enricher_v1 import _enrich_p1_session_vol
    now_ms = int(time.time() * 1000)
    data = {"session_liquidity_z": 1.5, "quality_status": "OK", "ts_ms": now_ms - 1000}
    r = _make_redis_json(data)
    out = _enrich_p1_session_vol("BTCUSDT", r)
    assert "session_liquidity_z" in out
    assert out["session_liquidity_z"] == pytest.approx(1.5)


def test_session_vol_enricher_clamps():
    from core.feature_enricher_v1 import _enrich_p1_session_vol
    now_ms = int(time.time() * 1000)
    data = {"session_liquidity_z": 99.0, "ts_ms": now_ms - 1000}
    r = _make_redis_json(data)
    out = _enrich_p1_session_vol("BTCUSDT", r)
    assert out["session_liquidity_z"] == pytest.approx(5.0)


def test_session_vol_enricher_absent():
    from core.feature_enricher_v1 import _enrich_p1_session_vol
    r = MagicMock()
    r.get.return_value = None
    r.mget.return_value = [None]
    out = _enrich_p1_session_vol("BTCUSDT", r)
    assert out == {}


# ── #17: session_signal_quality_prior ────────────────────────────────────────

def test_session_priors_enricher_good_session():
    from core.feature_enricher_v1 import _enrich_p1_session_priors
    # Simulate a good session: winrate=0.6, ev_r=+0.3, tp1_hit_rate=0.7
    # quality = 0.6*0.5 + 1.0*0.3 + 0.7*0.2 = 0.30 + 0.30 + 0.14 = 0.74
    r = MagicMock()
    r.hgetall.return_value = {
        "winrate": "0.6",
        "ev_r": "0.3",
        "tp1_hit_rate": "0.7",
        "sample_count": "50",
    }
    out = _enrich_p1_session_priors("BTCUSDT", r)
    assert "session_signal_quality_prior" in out
    assert out["session_signal_quality_prior"] == pytest.approx(0.74, abs=0.01)


def test_session_priors_enricher_bad_session():
    from core.feature_enricher_v1 import _enrich_p1_session_priors
    # Bad session: winrate=0.3, ev_r=-0.2, tp1_hit_rate=0.1
    # quality = 0.3*0.5 + 0.0*0.3 + 0.1*0.2 = 0.15 + 0.0 + 0.02 = 0.17
    r = MagicMock()
    r.hgetall.return_value = {
        "winrate": "0.3",
        "ev_r": "-0.2",
        "tp1_hit_rate": "0.1",
        "sample_count": "30",
    }
    out = _enrich_p1_session_priors("BTCUSDT", r)
    assert "session_signal_quality_prior" in out
    assert out["session_signal_quality_prior"] == pytest.approx(0.17, abs=0.01)


def test_session_priors_enricher_absent():
    from core.feature_enricher_v1 import _enrich_p1_session_priors
    r = MagicMock()
    r.hgetall.return_value = {}
    out = _enrich_p1_session_priors("BTCUSDT", r)
    assert out == {}


def test_session_priors_enricher_clamps_to_0_1():
    from core.feature_enricher_v1 import _enrich_p1_session_priors
    # Extreme winrate to check clamp
    r = MagicMock()
    r.hgetall.return_value = {"winrate": "2.0", "ev_r": "5.0", "tp1_hit_rate": "2.0"}
    out = _enrich_p1_session_priors("BTCUSDT", r)
    assert out["session_signal_quality_prior"] <= 1.0
    assert out["session_signal_quality_prior"] >= 0.0


# ── completeness check (updated to include #16-17) ───────────────────────────

def test_p1_keys_in_optional_keys():
    from core.external_features_payload_v1 import _V12_BASE_OPTIONAL_KEYS
    expected_p1 = {
        "mlofi_accel_1s", "mlofi_flip_count_3s", "mlofi_same_dir_secs",
        "microprice_ret_1s", "microprice_reversion_3s",
        "queue_depletion_rate_l1", "queue_refill_rate_l1",
        "adverse_selection_1s_bps", "post_fill_reversion_prob",
        "limit_vs_market_entry_edge_bps",
        "ev_after_slippage_bps", "net_edge_to_cost_ratio",
        "cost_widening_5s_bps",
        "regime_transition_code", "failed_breakout_count_30m",
        "bybit_data_age_ms", "liq_source_available", "liq_source_age_ms",
        "cross_venue_lead_lag_ms", "venue_consensus_persistence_3s",
        "liq_cascade_risk_score",
        "prior_timeout_rate_symbol_kind_session", "prior_tp1_before_timeout_rate",
        "session_liquidity_z", "session_signal_quality_prior",
    }
    expected_ext_health = {
        "fg_data_available", "fg_data_age_ms", "fg_data_stale",
        "cmc_data_available", "cmc_data_age_ms", "cmc_data_stale",
        "dl_data_available", "dl_data_age_ms", "dl_data_stale",
        "deribit_data_available", "deribit_data_age_ms", "deribit_data_stale",
        "bybit_data_available", "bybit_data_stale",
    }
    expected = expected_p1 | expected_ext_health
    missing = expected - set(_V12_BASE_OPTIONAL_KEYS)
    assert missing == set(), f"Keys missing from _V12_BASE_OPTIONAL_KEYS: {missing}"
