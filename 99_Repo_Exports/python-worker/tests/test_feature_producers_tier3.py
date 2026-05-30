"""Tier-3 feature producer tests: derived enricher, book_rate_ema, liquidation_ctx."""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

import core.feature_enricher_v1 as _enricher_mod
from core.feature_enricher_v1 import _enrich_derived, _enrich_book_rates
from services.book_rate_ema_producer import EmaState, classify_event
from services.liquidation_ctx_writer import parse_liq_event, get_liqmap_age_ms


# ─── Group A: derived features ────────────────────────────────────────────────


class TestDerivedEnricher:

    def test_vol_regime_code_known(self):
        out = _enrich_derived({"regime": "trending_bull"}, {})
        assert out["vol_regime_code"] == 1.0

    def test_vol_regime_code_range(self):
        out = _enrich_derived({"regime": "range"}, {})
        assert out["vol_regime_code"] == 3.0

    def test_vol_regime_code_na_emits_zero(self):
        out = _enrich_derived({"regime": "na"}, {})
        assert out["vol_regime_code"] == 0.0

    def test_vol_regime_unknown_string_no_emission(self):
        out = _enrich_derived({"regime": "weird_new_regime"}, {})
        assert "vol_regime_code" not in out

    def test_market_regime_alias(self):
        out = _enrich_derived({"market_regime": "trending_bear"}, {})
        assert out["vol_regime_code"] == 2.0

    def test_amihud_x_oi_delta_from_deriv_out(self):
        out = _enrich_derived(
            {"amihud_illiq": 0.01},
            {"open_interest_delta": 200.0},
        )
        assert abs(out["amihud_x_oi_delta"] - 2.0) < 1e-9

    def test_amihud_x_oi_delta_inline(self):
        # When deriv_out lacks oi_delta, fall back to indicators
        out = _enrich_derived(
            {"amihud_illiq": 0.01, "open_interest_delta": 200.0},
            {},
        )
        assert abs(out["amihud_x_oi_delta"] - 2.0) < 1e-9

    def test_amihud_alias_supported(self):
        out = _enrich_derived(
            {"amihud": 0.02, "open_interest_delta": 100.0}, {},
        )
        assert abs(out["amihud_x_oi_delta"] - 2.0) < 1e-9

    def test_amihud_missing_no_emission(self):
        out = _enrich_derived({"open_interest_delta": 100.0}, {})
        assert "amihud_x_oi_delta" not in out

    def test_conf_ma_ratio_basic(self):
        out = _enrich_derived({"confidence_v1": 0.8, "confidence_ema": 0.6}, {})
        assert abs(out["conf_ma_ratio"] - (0.8 / 0.6)) < 1e-9

    def test_conf_ma_ratio_zero_denominator_skipped(self):
        out = _enrich_derived({"confidence_v1": 0.8, "confidence_ema": 0.0}, {})
        assert "conf_ma_ratio" not in out

    def test_confidence_x_of_score(self):
        out = _enrich_derived({"confidence_v1": 0.7, "of_confirm_score": 0.5}, {})
        assert abs(out["confidence_x_of_score"] - 0.35) < 1e-9

    def test_gate_hardness_score_basic(self):
        indicators = {
            "atr_floor_ready": True,
            "regime_ok": True,
            "strong_gate_ok": False,
            "liq_gate_passed": True,
            "absorption_ok": True,  # 4/5 pass
        }
        out = _enrich_derived(indicators, {})
        assert abs(out["gate_hardness_score"] - 0.8) < 1e-9

    def test_gate_hardness_no_signal_no_emission(self):
        # Only 2 gates — below min_total threshold of 3
        out = _enrich_derived({"a_ok": True, "b_ok": False}, {})
        assert "gate_hardness_score" not in out

    def test_gate_hardness_ints_as_bools(self):
        out = _enrich_derived({
            "x_ok": 1, "y_ok": 0, "z_ok": 1, "w_ok": 1,
        }, {})
        assert abs(out["gate_hardness_score"] - 0.75) < 1e-9

    def test_model_calibration_err_direct(self):
        out = _enrich_derived({"ml_shadow_conf01": 0.15}, {})
        assert abs(out["model_calibration_err"] - 0.35) < 1e-9

    def test_model_calibration_err_from_breakdown(self):
        out = _enrich_derived({
            "confidence_breakdown": {"ml_shadow_conf01": 0.85},
        }, {})
        assert abs(out["model_calibration_err"] - 0.35) < 1e-9

    def test_model_calibration_err_missing_skip(self):
        out = _enrich_derived({"confidence_v1": 0.5}, {})
        assert "model_calibration_err" not in out

    def test_rsi_cvd_uptrending_returns_high(self):
        # 14 increasing CVD values → all gains → RSI=100
        cvd_series = [float(i) for i in range(15)]
        out = _enrich_derived({"cvd_series": cvd_series}, {})
        assert out["rsi_cvd"] == 100.0

    def test_rsi_cvd_downtrending_returns_low(self):
        cvd_series = [float(15 - i) for i in range(15)]  # 15,14,...,1
        out = _enrich_derived({"cvd_series": cvd_series}, {})
        assert abs(out["rsi_cvd"]) < 1e-9  # all losses → RSI=0

    def test_rsi_cvd_short_series_no_emission(self):
        out = _enrich_derived({"cvd_series": [1, 2, 3]}, {})
        assert "rsi_cvd" not in out

    def test_rsi_cvd_history_alias(self):
        out = _enrich_derived({"cvd_history": [float(i) for i in range(15)]}, {})
        assert "rsi_cvd" in out


# ─── Group B: book_rate_ema_producer ──────────────────────────────────────────


class TestBookRateProducer:

    def test_classify_event_add(self):
        assert classify_event({"event_type": "add"}) == "add"
        assert classify_event({"type": "depth_add"}) == "add"

    def test_classify_event_cancel(self):
        assert classify_event({"event_type": "cancel"}) == "cancel"
        assert classify_event({"e": "DELETE"}) == "cancel"

    def test_classify_event_trade(self):
        assert classify_event({"event_type": "trade"}) == "trade"
        assert classify_event({"type": "fill"}) == "trade"

    def test_classify_event_unknown_defaults_update(self):
        assert classify_event({}) == "update"

    def test_classify_event_from_delta_positive(self):
        assert classify_event({"qty_delta": "5.0"}) == "add"

    def test_classify_event_from_delta_negative(self):
        assert classify_event({"qty_delta": "-3.0"}) == "cancel"

    def test_ema_first_update_seeds(self):
        st = EmaState()
        st.update(1.0, 1000)
        assert st.value == 1.0
        assert st.last_update_ms == 1000

    def test_ema_decay_over_half_life(self):
        # With HALF_LIFE_SEC=60, after 60s the previous value halves
        st = EmaState()
        st.update(2.0, 1_000_000)
        st.update(0.0, 1_060_000)
        # 2.0 * exp(-ln2 * 1) = 2.0 * 0.5 = 1.0
        assert abs(st.value - 1.0) < 0.05

    def test_ema_decayed_when_idle(self):
        st = EmaState()
        st.update(2.0, 1_000_000)
        # No further updates, query 60s later
        rate = st.decayed(1_060_000)
        # decay halves + scales by decay_per_sec
        assert rate > 0
        assert rate < 2.0


# ─── Group C: liquidation_ctx_writer ──────────────────────────────────────────


class TestLiquidationParser:

    def test_parse_with_usd_direct(self):
        out = parse_liq_event({"symbol": "BTCUSDT", "usd": "10000", "ts_ms": "1700"})
        assert out is not None
        sym, usd, ts = out
        assert sym == "BTCUSDT"
        assert usd == 10000.0

    def test_parse_alias_value_usd(self):
        out = parse_liq_event({"s": "ETHUSDT", "value_usd": "5000"})
        assert out is not None
        assert out[0] == "ETHUSDT"
        assert out[1] == 5000.0

    def test_parse_derive_from_qty_price(self):
        out = parse_liq_event({"symbol": "SOL", "q": "100", "p": "150"})
        assert out is not None
        _, usd, _ = out
        assert usd == 15000.0

    def test_parse_no_symbol_rejected(self):
        assert parse_liq_event({"usd": "100"}) is None

    def test_parse_zero_usd_rejected(self):
        assert parse_liq_event({"symbol": "X", "usd": "0"}) is None

    def test_parse_ts_fallback_to_now(self):
        out = parse_liq_event({"symbol": "B", "usd": "100"})
        assert out is not None
        # ts_ms ≥ recent (within 5s)
        assert out[2] > 0

    def test_get_liqmap_age_present(self):
        now_ms = int(time.time() * 1000)
        payload = {"ts_ms": now_ms - 5000}
        r = MagicMock()
        r.get.side_effect = lambda k: json.dumps(payload) if k == "liqmap:snapshot:BTC" else None
        age = get_liqmap_age_ms(r, "BTC")
        assert age is not None
        assert 4500 < age < 6000

    def test_get_liqmap_age_missing(self):
        r = MagicMock()
        r.get.return_value = None
        assert get_liqmap_age_ms(r, "BTC") is None

    def test_get_liqmap_age_alt_key(self):
        now_ms = int(time.time() * 1000)
        r = MagicMock()
        # Only the alt-format key has data
        r.get.side_effect = lambda k: (
            json.dumps({"ts_ms": now_ms - 1000}) if k == "liqmap:BTC:1h" else None
        )
        age = get_liqmap_age_ms(r, "BTC")
        assert age is not None and age >= 0


# ─── _enrich_book_rates wiring ────────────────────────────────────────────────


class TestEnrichBookRates:

    def setup_method(self):
        _enricher_mod._snapshot_cache.clear()

    def test_reads_book_rates_snapshot(self):
        now = int(time.time() * 1000)
        payload = {
            "depth_pull_ratio": 0.8,
            "cancel_to_fill_ratio": 1.2,
            "maker_cancel_ratio": 0.75,
            "book_refresh_rate_hz": 25.0,
            "added_bid_rate_ema": 5.0,
            "ts_ms": now,
        }
        r = MagicMock()
        r.get.return_value = json.dumps(payload)
        out = _enrich_book_rates("BTCUSDT", r)
        assert out["depth_pull_ratio"] == 0.8
        assert out["cancel_to_fill_ratio"] == 1.2
        assert out["maker_cancel_ratio"] == 0.75
        assert out["book_refresh_rate_hz"] == 25.0
        assert out["added_bid_rate_ema"] == 5.0

    def test_empty_when_missing(self):
        r = MagicMock()
        r.get.return_value = None
        assert _enrich_book_rates("BTCUSDT", r) == {}

    def test_no_symbol_returns_empty(self):
        assert _enrich_book_rates("", MagicMock()) == {}

    def test_no_redis_returns_empty(self):
        assert _enrich_book_rates("BTC", None) == {}
