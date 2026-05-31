"""Tests for 6 feature producer services + microstructure_metrics_v2 pure fns.

Focus: pure stats correctness + parse_trade boundaries. Service event-loops
are smoke-checked via unit-level helpers; full e2e is live verification.
"""
from __future__ import annotations

import math

import pytest

# ─── execution_stats_updater ─────────────────────────────────────────────────


from services.execution_stats_updater import (
    parse_trade, compute_stats, TradeRecord,
)


def _trade(r=0.5, pnl=10.0, risk_bps=20.0, slip=2.0, fill=120, adverse=300, ts=1000):
    return TradeRecord(ts_ms=ts, r_multiple=r, pnl_net=pnl,
                       risk_bps=risk_bps, slippage_bps=slip,
                       fill_ms=fill, adverse_ms=adverse)


class TestParseTrade:
    def test_basic_extract(self):
        fields = {"symbol": "BTCUSDT", "r_multiple": "0.5", "pnl_net": "12.0",
                  "risk_bps": "15", "slippage_bps": "3", "exit_ts_ms": "1700"}
        out = parse_trade(fields)
        assert out is not None
        sym, rec = out
        assert sym == "BTCUSDT"
        assert rec.r_multiple == 0.5
        assert rec.pnl_net == 12.0
        assert rec.risk_bps == 15.0

    def test_derive_risk_from_entry_sl(self):
        fields = {"symbol": "ETH", "r_multiple": "1.0", "entry": "1000", "sl": "990"}
        out = parse_trade(fields)
        assert out is not None
        _, rec = out
        # 10 bps = 1% / 100 = 100 bps? actually |entry-sl|/entry*10000 = 10/1000*10000 = 100
        assert rec.risk_bps == 100.0

    def test_missing_symbol_rejected(self):
        assert parse_trade({"r_multiple": "0.5"}) is None

    def test_invalid_r_rejected(self):
        assert parse_trade({"symbol": "X", "r_multiple": "nan"}) is None
        assert parse_trade({"symbol": "X", "r_multiple": ""}) is None


class TestComputeStats:
    def test_below_min_returns_empty(self):
        out = compute_stats([_trade()] * 5)
        assert out == {}

    def test_expectancy_bps(self):
        # 12 trades: avg r=0.5, risk=20 → expectancy = 0.5*20 = 10 bps
        out = compute_stats([_trade(r=0.5, risk_bps=20.0)] * 12)
        assert abs(out["expectancy_bps"] - 10.0) < 1e-6

    def test_profit_factor_roll20(self):
        trades = [_trade(r=0.5, pnl=10) for _ in range(10)] + [_trade(r=-0.5, pnl=-5) for _ in range(10)]
        out = compute_stats(trades)
        # winners 100 / losers 50 = 2.0
        assert abs(out["profit_factor_roll20"] - 2.0) < 1e-6

    def test_kelly_fraction_positive_edge(self):
        # 15 wins r=1, 5 losses r=-1 → win_rate=0.75, b=1 → kelly = 0.5
        trades = [_trade(r=1.0, pnl=10) for _ in range(15)] + [_trade(r=-1.0, pnl=-10) for _ in range(5)]
        out = compute_stats(trades)
        assert abs(out["kelly_fraction_roll"] - 0.5) < 1e-6

    def test_kelly_clamped_at_neg_half(self):
        # All losses → kelly heavily negative, clamped to -0.5
        trades = [_trade(r=-1.0, pnl=-10) for _ in range(15)] + [_trade(r=0.1, pnl=1) for _ in range(5)]
        out = compute_stats(trades)
        assert out["kelly_fraction_roll"] >= -0.5

    def test_recovery_factor_no_drawdown(self):
        # All wins → no drawdown → recovery = cum
        trades = [_trade(r=0.5) for _ in range(12)]
        out = compute_stats(trades)
        # cum=6, max_dd=0 → recovery=6 (special case)
        assert out["recovery_factor_roll"] == 6.0

    def test_fill_time_p90(self):
        # Latencies 10,20,...,200ms; p90 ≈ 180ms
        trades = [_trade(fill=i * 10) for i in range(1, 21)]
        out = compute_stats(trades)
        assert out["fill_time_p90_ms"] >= 170

    def test_slippage_mean(self):
        trades = [_trade(slip=s) for s in (1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2)]
        out = compute_stats(trades)
        assert abs(out["slippage_realized_bps"] - 33/12) < 1e-6


# ─── fear_greed_exporter ──────────────────────────────────────────────────────


from services.fear_greed_exporter import build_snapshot, _CLASSIFICATION_BREADTH


class TestFearGreedBuild:
    def test_normalisation(self):
        snap = build_snapshot({"value": 75, "classification": "Greed"})
        assert snap["value"] == 75
        assert snap["market_breadth_score"] == _CLASSIFICATION_BREADTH["Greed"]

    def test_unknown_classification_neutral_breadth(self):
        snap = build_snapshot({"value": 50, "classification": "Unknown"})
        assert snap["market_breadth_score"] == 0.5

    def test_missing_value_falls_back(self):
        snap = build_snapshot({})
        assert snap["value"] == 50


# ─── crossasset_ctx_writer ────────────────────────────────────────────────────


from services.crossasset_ctx_writer import (
    _pearson, _stdev, returns_from_closes, _parse_kline_close,
)


class TestCrossassetStats:
    def test_pearson_perfect_corr(self):
        assert abs(_pearson([1, 2, 3, 4], [2, 4, 6, 8]) - 1.0) < 1e-9

    def test_pearson_inverse_corr(self):
        assert abs(_pearson([1, 2, 3, 4], [-1, -2, -3, -4]) - (-1.0)) < 1e-9

    def test_pearson_zero_var(self):
        assert _pearson([1, 1, 1], [1, 2, 3]) == 0.0

    def test_stdev_basic(self):
        assert abs(_stdev([1, 2, 3, 4, 5]) - math.sqrt(2.5)) < 1e-9

    def test_returns_from_closes(self):
        closes = [100.0, 110.0, 121.0]
        rets = returns_from_closes(closes)
        assert len(rets) == 2
        assert abs(rets[0] - math.log(1.1)) < 1e-9

    def test_parse_kline_close_variants(self):
        assert _parse_kline_close({"close": "100.5"}) == 100.5
        assert _parse_kline_close({"c": "99.9"}) == 99.9
        assert _parse_kline_close({"close_price": "1.5"}) == 1.5
        assert _parse_kline_close({}) is None


# ─── microstructure_metrics_v2 ───────────────────────────────────────────────


from core.microstructure_metrics_v2 import (
    OHLCBar, MinuteBarAggregator,
    kyle_lambda, vpin_rolling, tick_autocorr_lag1, roll_spread_est,
    hurst_exp, ohlc_vol_estimators, amihud_illiquidity, pin_estimate_from_flow,
    compute_all,
)


class TestKyleLambda:
    def test_positive_impact(self):
        # Δprice positively correlated with signed-volume → λ > 0
        # Big trades cause big moves
        prices = [100, 100.1, 100.5, 100.7, 100.9]
        svols = [0, 1, 5, 2, 3]  # aligned diffs: 0.1, 0.4, 0.2, 0.2
        assert kyle_lambda(prices, svols) > 0

    def test_zero_when_no_variance(self):
        prices = [100, 100, 100, 100]
        svols = [1, 1, 1, 1]
        assert kyle_lambda(prices, svols) == 0.0

    def test_handles_short_input(self):
        assert kyle_lambda([100], [1]) == 0.0


class TestVpinRolling:
    def test_balanced_flow(self):
        # Equal buy/sell → imbalance ~ 0 per bucket
        buys = [10.0] * 50
        sells = [10.0] * 50
        v = vpin_rolling(buys, sells, n_buckets=10)
        assert v < 0.2  # near zero

    def test_all_buys(self):
        buys = [10.0] * 50
        sells = [0.0] * 50
        v = vpin_rolling(buys, sells, n_buckets=10)
        assert v > 0.9  # near 1

    def test_empty_input(self):
        assert vpin_rolling([], []) == 0.0


class TestTickAutocorr:
    def test_strong_persistence(self):
        # Persistent series: 100, 101, 102, 103, 104, 105
        # diffs: 1, 1, 1, 1, 1 — perfect correlation between consecutive diffs
        prices = [100, 101, 102, 103, 104, 105, 106]
        ac = tick_autocorr_lag1(prices)
        # All diffs equal → variance=0 → corr returns 0.0
        assert ac == 0.0

    def test_mean_reversion(self):
        # Up-down pattern: ac should be negative
        prices = [100, 101, 100, 101, 100, 101, 100]
        ac = tick_autocorr_lag1(prices)
        assert ac < 0.0


class TestRollSpreadEst:
    def test_positive_when_negative_serial_cov(self):
        # Bid-ask bounce pattern → negative serial cov
        prices = [100.0, 100.1, 100.0, 100.1, 100.0, 100.1, 100.0]
        rs = roll_spread_est(prices)
        assert rs > 0

    def test_zero_when_trending(self):
        # Pure trend → positive serial cov → unidentified → 0
        prices = [100, 101, 102, 103, 104, 105, 106]
        assert roll_spread_est(prices) == 0.0


class TestHurst:
    def test_random_walk_close_to_half(self):
        import random
        random.seed(42)
        prices = [100.0]
        for _ in range(80):
            prices.append(prices[-1] * (1 + random.gauss(0, 0.001)))
        h = hurst_exp(prices)
        # Random walk → H ≈ 0.5; allow generous bounds
        assert 0.2 < h < 0.8

    def test_short_input_returns_neutral(self):
        assert hurst_exp([100, 101]) == 0.5


class TestOHLCEstimators:
    def _bars(self, n: int = 25) -> list[OHLCBar]:
        bars = []
        px = 100.0
        for i in range(n):
            o = px
            h = px * 1.002
            l = px * 0.998
            c = px * 1.001
            bars.append(OHLCBar(o=o, h=h, l=l, c=c, volume=1000.0 + i, ts_ms=i * 60_000))
            px = c
        return bars

    def test_vol_estimators_positive(self):
        vol = ohlc_vol_estimators(self._bars())
        assert vol["garman_klass_vol"] > 0.0
        assert vol["parkinson_vol"] > 0.0
        assert vol["yang_zhang_vol"] > 0.0

    def test_amihud_positive(self):
        assert amihud_illiquidity(self._bars()) > 0.0

    def test_pin_balanced_near_zero(self):
        buys = [10.0] * 30
        sells = [10.0] * 30
        assert pin_estimate_from_flow(buys, sells) < 0.05

    def test_pin_imbalanced_high(self):
        buys = [100.0] * 30
        sells = [1.0] * 30
        assert pin_estimate_from_flow(buys, sells) > 0.8

    def test_minute_bar_aggregator(self):
        agg = MinuteBarAggregator()
        base = 1_700_000_000_000
        for i in range(5):
            agg.on_tick(100.0 + i, 1.0, base + i * 1000)
        assert len(agg.bars()) == 0
        agg.on_tick(105.0, 1.0, base + 60_001)
        assert len(agg.bars()) == 1
        assert agg.bars()[0].c == 104.0


class TestComputeAll:
    def test_emits_all_when_full_inputs(self):
        prices = [100 + i * 0.1 for i in range(50)]
        svols = [(1 if i % 2 == 0 else -1) * 10 for i in range(50)]
        takers = [(1 if i % 3 == 0 else -0.5) * 5 for i in range(50)]
        buys = [10.0] * 50
        sells = [5.0] * 50
        out = compute_all(
            prices=prices, signed_vols=svols, taker_signed_vols=takers,
            buy_vols=buys, sell_vols=sells, funding_rate=0.0001, vol_regime_code=1.0,
        )
        assert "kyle_lambda" in out
        assert "vpin_rolling" in out
        assert "vpin_x_funding" in out
        assert "kyle_x_vpin" in out
        assert "tick_autocorr_lag1" in out
        assert "hurst_exp_50" in out
        assert "hurst_x_vol_regime" in out
        bars = TestOHLCEstimators()._bars()
        out2 = compute_all(
            prices=prices, signed_vols=svols, buy_vols=buys, sell_vols=sells, bars=bars,
        )
        assert out2["garman_klass_vol"] > 0.0
        assert out2["amihud_illiquidity"] > 0.0
        assert "pin_estimate" in out2

    def test_minimal_inputs(self):
        out = compute_all(prices=[], signed_vols=[])
        assert out["garman_klass_vol"] == 0.0
        assert out["pin_estimate"] == 0.0


# ─── orderflow_pressure_v2 ────────────────────────────────────────────────────


from services.orderflow_pressure_v2 import (
    trade_freq_per_hr, skewness, ofi_features,
)


class TestOFPressureV2:
    def test_trade_freq(self):
        # 100 trades in 300s → 100 * 12 = 1200/hr
        assert abs(trade_freq_per_hr(100, 300) - 1200.0) < 1e-9

    def test_skewness_symmetric_zero(self):
        # Symmetric distribution → skew ≈ 0
        assert abs(skewness([1, 2, 3, 4, 5, 4, 3, 2, 1])) < 0.5

    def test_skewness_positive_tail(self):
        # Right-skewed distribution
        s = skewness([1, 1, 1, 1, 1, 1, 1, 10])
        assert s > 0

    def test_skewness_degenerate(self):
        assert skewness([5, 5, 5]) == 0.0

    def test_ofi_features_emits(self):
        svols = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
        ts = [1000 * i for i in range(10)]
        out = ofi_features(svols, ts)
        assert "ofi" in out
        assert out["ofi"] == 10.0


# ─── sweep_detector_v2 ────────────────────────────────────────────────────────


from services.sweep_detector_v2 import (
    compute_source_jump_usd, compute_sweep_velocity_bps_s,
    compute_cvd, compute_sweep_div_match,
)


def _tick(ts, px, qty, sv):
    return (ts, px, qty, sv)


class TestSweepDetectorV2:
    def test_jump_usd(self):
        ticks = [
            _tick(1000, 100.0, 1.0, 0),
            _tick(2000, 100.5, 5.0, 0),  # 0.5 × 5 = 2.5 USD
            _tick(3000, 100.5, 1.0, 0),
        ]
        assert compute_source_jump_usd(ticks) == 2.5

    def test_sweep_velocity_upward(self):
        # 100 → 110 over 5 seconds = 1000 bps / 5s = 200 bps/s
        ticks = [
            _tick(0, 100.0, 1.0, 0),
            _tick(1000, 102.0, 1.0, 0),
            _tick(2000, 104.0, 1.0, 0),
            _tick(3000, 106.0, 1.0, 0),
            _tick(4000, 108.0, 1.0, 0),
            _tick(5000, 110.0, 1.0, 0),
        ]
        v, d = compute_sweep_velocity_bps_s(ticks)
        assert d == 1
        assert v > 100  # ~200 bps/s

    def test_sweep_no_significant_move(self):
        # Tiny moves below threshold (default 5 bps)
        ticks = [_tick(i * 1000, 100.0 + i * 0.001, 1.0, 0) for i in range(10)]
        v, d = compute_sweep_velocity_bps_s(ticks)
        assert d == 0

    def test_div_match_up_with_buying(self):
        ticks = [_tick(0, 100, 1, 1), _tick(1000, 110, 1, 1), _tick(2000, 110, 1, 1)]
        # CVD = +2 (buying), direction up = +1 → match
        assert compute_sweep_div_match(ticks, 1) == 1.0

    def test_div_no_match_up_with_selling(self):
        ticks = [_tick(0, 100, 1, -1), _tick(1000, 110, 1, -1)]
        # CVD = -2 (selling), direction up = +1 → no match
        assert compute_sweep_div_match(ticks, 1) == 0.0

    def test_div_no_sweep_returns_zero(self):
        ticks = [_tick(0, 100, 1, 1)]
        assert compute_sweep_div_match(ticks, 0) == 0.0
