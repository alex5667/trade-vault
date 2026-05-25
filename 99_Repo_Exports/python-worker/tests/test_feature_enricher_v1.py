"""Tests for core/feature_enricher_v1.py."""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

import pytest

from core.feature_enricher_v1 import (
    enrich_indicators,
    _enrich_deriv_ctx,
    _enrich_crossasset_ctx,
    _enrich_sentiment,
    _enrich_book_features,
    _enrich_microbar,
    _enrich_momentum,
    _enrich_vol_features,
    _enrich_execution_stats,
    _enrich_liquidation_ctx,
    _safe_float,
    _load_json_snapshot,
    _prime_snapshot_cache,
    _snapshot_keys_for_symbol,
    _STUBS_CONDITIONAL,
    _STUBS_PRODUCER_BACKED,
)
import core.feature_enricher_v1 as _enricher_mod


# ─── Helpers ─────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _clear_snapshot_cache():
    """Prevent in-process cache from leaking between tests."""
    _enricher_mod._snapshot_cache.clear()
    yield
    _enricher_mod._snapshot_cache.clear()


def _fresh_ts() -> int:
    return int(time.time() * 1000)


def _mk_redis_with(key_payload: dict[str, str]) -> MagicMock:
    """Build a MagicMock redis client where .get(k) → key_payload[k] (or None)."""
    r = MagicMock()
    r.get.side_effect = lambda k: key_payload.get(k)
    r.hgetall.side_effect = lambda k: {}
    return r


# ─── _safe_float ─────────────────────────────────────────────────────────────


class TestSafeFloat:
    def test_finite(self):
        assert _safe_float(3.14) == 3.14
        assert _safe_float("1.5") == 1.5

    def test_none_returns_default(self):
        assert _safe_float(None) == 0.0
        assert _safe_float(None, default=-1.0) == -1.0

    def test_invalid_returns_default(self):
        assert _safe_float("abc") == 0.0
        assert _safe_float(float("nan")) == 0.0
        assert _safe_float(float("inf")) == 0.0


# ─── _load_json_snapshot ─────────────────────────────────────────────────────


class TestLoadJsonSnapshot:
    def test_missing_key_returns_empty(self):
        r = _mk_redis_with({})
        assert _load_json_snapshot(r, "missing", 60000) == {}

    def test_fresh_data_returned(self):
        payload = {"value": 42, "ts_ms": _fresh_ts()}
        r = _mk_redis_with({"k": json.dumps(payload)})
        out = _load_json_snapshot(r, "k", 60000)
        assert out["value"] == 42

    def test_stale_data_dropped(self):
        old_ts = _fresh_ts() - 7200_000
        payload = {"value": 42, "ts_ms": old_ts}
        r = _mk_redis_with({"k": json.dumps(payload)})
        assert _load_json_snapshot(r, "k", 60000) == {}

    def test_invalid_json_returns_empty(self):
        r = _mk_redis_with({"k": "not json {"})
        assert _load_json_snapshot(r, "k", 60000) == {}

    def test_no_ts_ms_accepted(self):
        # Some snapshots have no ts — accept (no freshness check possible)
        payload = {"value": 42}
        r = _mk_redis_with({"k": json.dumps(payload)})
        assert _load_json_snapshot(r, "k", 60000)["value"] == 42

    def test_cache_prevents_second_redis_get(self):
        # Second call within TTL must not hit Redis again
        _enricher_mod._snapshot_cache.clear()
        payload = {"value": 7, "ts_ms": _fresh_ts()}
        r = _mk_redis_with({"ck": json.dumps(payload)})
        _load_json_snapshot(r, "ck", 60000)
        _load_json_snapshot(r, "ck", 60000)
        assert r.get.call_count == 1  # only one Redis GET despite two calls

    def test_cache_respects_staleness_on_hit(self):
        # Even on cache-hit, ts_ms staleness guard still rejects old data
        _enricher_mod._snapshot_cache.clear()
        old_ts = _fresh_ts() - 200_000
        payload = {"value": 9, "ts_ms": old_ts}
        raw = json.dumps(payload)
        r = _mk_redis_with({"sk": raw})
        # Prime cache with stale data (first call returns {} due to stale)
        first = _load_json_snapshot(r, "sk", 60000)
        assert first == {}
        # Cache holds empty dict — second call is still {}
        second = _load_json_snapshot(r, "sk", 60000)
        assert second == {}
        assert r.get.call_count == 1  # only one Redis hit


# ─── _prime_snapshot_cache (MGET, zero time-skew) ────────────────────────────


class TestPrimeSnapshotCache:
    def test_mget_used_not_get(self):
        # _prime_snapshot_cache must call mget, not individual gets
        ts = _fresh_ts()
        r = MagicMock()
        r.mget.return_value = [
            json.dumps({"val": 1, "ts_ms": ts}),
            json.dumps({"val": 2, "ts_ms": ts}),
        ]
        _prime_snapshot_cache(r, ["k1", "k2"])
        r.mget.assert_called_once_with(["k1", "k2"])
        assert r.get.call_count == 0  # no individual GETs

    def test_primed_keys_served_from_cache(self):
        ts = _fresh_ts()
        r = MagicMock()
        r.mget.return_value = [json.dumps({"funding_rate": 0.0001, "ts_ms": ts})]
        _prime_snapshot_cache(r, ["ctx:deriv:ETHUSDT"])
        # Second access via _load_json_snapshot must NOT hit Redis again
        r2 = MagicMock()  # fresh mock — any call would be a miss
        data = _load_json_snapshot(r2, "ctx:deriv:ETHUSDT", 60000)
        assert data.get("funding_rate") == 0.0001
        assert r2.get.call_count == 0

    def test_expired_key_refetched(self):
        # If a key was cached as {} (miss), prime should refetch it
        _enricher_mod._snapshot_cache["stale_k"] = ({}, 0)  # expired entry
        ts = _fresh_ts()
        r = MagicMock()
        r.mget.return_value = [json.dumps({"x": 99, "ts_ms": ts})]
        _prime_snapshot_cache(r, ["stale_k"])
        r.mget.assert_called_once()
        assert _enricher_mod._snapshot_cache["stale_k"][0].get("x") == 99

    def test_all_symbol_keys_covered(self):
        # Ensure _snapshot_keys_for_symbol returns a non-empty list covering
        # the main producers
        keys = _snapshot_keys_for_symbol("SOLUSDT")
        assert any("deriv" in k for k in keys)
        assert any("crossasset" in k for k in keys)
        assert any("fear_greed" in k for k in keys)
        assert any("microstruct" in k for k in keys)

    def test_enrich_indicators_uses_mget_not_get(self):
        # Full enrich_indicators must use MGET (via _prime_snapshot_cache),
        # not individual GETs for snapshot keys
        ts = _fresh_ts()
        r = MagicMock()
        r.mget.return_value = [None] * len(_snapshot_keys_for_symbol("ETHUSDT"))
        enrich_indicators(indicators={}, symbol="ETHUSDT", redis_client=r)
        assert r.mget.call_count >= 1
        assert r.get.call_count == 0


# ─── Stub health tracking ─────────────────────────────────────────────────────


class TestStubHealth:
    def test_conditional_stub_silent(self, caplog):
        # Conditional stubs (iceberg_refresh etc.) must not emit warnings
        import logging
        _enricher_mod._stub_miss_last_warn.clear()
        _enricher_mod._stub_miss_total.clear()
        with caplog.at_level(logging.WARNING, logger="core.feature_enricher_v1"):
            out = enrich_indicators(indicators={}, symbol="ETHUSDT", redis_client=None)
        # No warning for conditional stubs
        warns = [r for r in caplog.records if "iceberg_refresh" in r.message]
        assert warns == []
        # But the stub value IS present
        assert out.get("iceberg_refresh") == 0.0

    def test_producer_backed_stub_warns(self, caplog):
        import logging
        _enricher_mod._stub_miss_last_warn.clear()
        _enricher_mod._stub_miss_total.clear()
        # Bypass startup grace so warnings are emitted
        orig_start = _enricher_mod._ENRICHER_START_TIME
        _enricher_mod._ENRICHER_START_TIME = 0.0
        try:
            with caplog.at_level(logging.WARNING, logger="core.feature_enricher_v1"):
                enrich_indicators(indicators={}, symbol="ETHUSDT", redis_client=None)
        finally:
            _enricher_mod._ENRICHER_START_TIME = orig_start
        warns = [r.message for r in caplog.records if "producer-backed stub" in r.message]
        # At least one producer-backed feature should have warned
        assert len(warns) > 0
        assert "ETHUSDT" in warns[0]

    def test_producer_backed_warn_rate_limited(self, caplog):
        import logging
        _enricher_mod._stub_miss_last_warn.clear()
        _enricher_mod._stub_miss_total.clear()
        orig_start = _enricher_mod._ENRICHER_START_TIME
        _enricher_mod._ENRICHER_START_TIME = 0.0
        try:
            # First call: warns
            with caplog.at_level(logging.WARNING, logger="core.feature_enricher_v1"):
                enrich_indicators(indicators={}, symbol="ETHUSDT", redis_client=None)
            first_count = len([r for r in caplog.records if "producer-backed stub" in r.message])
            caplog.clear()
            # Second call within warn interval: silent
            with caplog.at_level(logging.WARNING, logger="core.feature_enricher_v1"):
                enrich_indicators(indicators={}, symbol="ETHUSDT", redis_client=None)
            second_count = len([r for r in caplog.records if "producer-backed stub" in r.message])
        finally:
            _enricher_mod._ENRICHER_START_TIME = orig_start
        assert first_count > 0
        assert second_count == 0  # rate-limited

    def test_stub_miss_counter_increments(self):
        _enricher_mod._stub_miss_total.clear()
        enrich_indicators(indicators={}, symbol="ETHUSDT", redis_client=None)
        enrich_indicators(indicators={}, symbol="ETHUSDT", redis_client=None)
        # At least one producer-backed feature should have count >= 2
        assert any(v >= 2 for v in _enricher_mod._stub_miss_total.values())

    def test_conditional_and_producer_backed_disjoint(self):
        assert _STUBS_CONDITIONAL.isdisjoint(_STUBS_PRODUCER_BACKED)

    def test_all_stubs_in_one_of_two_sets(self):
        all_stubs = _STUBS_CONDITIONAL | _STUBS_PRODUCER_BACKED
        for key in ("iceberg_refresh", "roll_spread_est", "crypto_fear_greed",
                    "expectancy_bps", "liquidation_usd_1m"):
            assert key in all_stubs


# ─── _enrich_deriv_ctx ───────────────────────────────────────────────────────


class TestDerivCtx:
    def test_no_symbol_returns_empty(self):
        assert _enrich_deriv_ctx("", MagicMock()) == {}

    def test_no_redis_returns_empty(self):
        assert _enrich_deriv_ctx("BTCUSDT", None) == {}

    def test_funding_rate_converted_to_bps(self):
        payload = {"funding_rate": 0.0001, "ts_ms": _fresh_ts()}
        r = _mk_redis_with({"ctx:deriv:BTCUSDT": json.dumps(payload)})
        out = _enrich_deriv_ctx("BTCUSDT", r)
        # 0.0001 = 1 bp
        assert abs(out["funding_rate_bps"] - 1.0) < 1e-9

    def test_oi_delta_passed_through(self):
        payload = {"delta_oi_5m": 1234567.0, "ts_ms": _fresh_ts()}
        r = _mk_redis_with({"ctx:deriv:BTCUSDT": json.dumps(payload)})
        out = _enrich_deriv_ctx("BTCUSDT", r)
        assert out["open_interest_delta"] == 1234567.0

    def test_legacy_oi_key_supported(self):
        payload = {"oi_delta_5m": 999.0, "ts_ms": _fresh_ts()}
        r = _mk_redis_with({"ctx:deriv:BTCUSDT": json.dumps(payload)})
        out = _enrich_deriv_ctx("BTCUSDT", r)
        assert out["open_interest_delta"] == 999.0


# ─── _enrich_crossasset_ctx ──────────────────────────────────────────────────


class TestCrossassetCtx:
    def test_btc_corr_emitted(self):
        payload = {"btc_corr_5m": 0.72, "alt_season_index": 0.55,
                   "ts_ms": _fresh_ts()}
        r = _mk_redis_with({"crossasset:ctx:ETHUSDT": json.dumps(payload)})
        out = _enrich_crossasset_ctx("ETHUSDT", r)
        assert out["btc_corr_5m"] == 0.72
        assert out["alt_season_index"] == 0.55

    def test_legacy_corr_key_supported(self):
        payload = {"corr_btc_5m": 0.5, "ts_ms": _fresh_ts()}
        r = _mk_redis_with({"crossasset:ctx:SOLUSDT": json.dumps(payload)})
        out = _enrich_crossasset_ctx("SOLUSDT", r)
        assert out["btc_corr_5m"] == 0.5

    def test_fallback_to_legacy_key(self):
        payload = {"btc_corr_5m": 0.61, "ts_ms": _fresh_ts()}
        r = MagicMock()
        # ctx-key empty, fallback corr-key has data
        r.get.side_effect = lambda k: json.dumps(payload) if k == "crossasset:corr:SOLUSDT" else None
        out = _enrich_crossasset_ctx("SOLUSDT", r)
        assert out["btc_corr_5m"] == 0.61

    def test_empty_when_no_keys(self):
        r = _mk_redis_with({})
        assert _enrich_crossasset_ctx("BTC", r) == {}

    def test_btcusdt_self_ref_padding(self):
        # No crossasset snapshot for BTC (writer skips self) → degenerate self-ref
        glob = {"alt_season_index": 0.42, "ts_ms": _fresh_ts()}
        r = _mk_redis_with({"crossasset:ctx:_global": json.dumps(glob)})
        out = _enrich_crossasset_ctx("BTCUSDT", r)
        assert out["btc_corr_5m"] == 1.0
        assert out["cross_asset_vol_ratio"] == 1.0
        assert out["alt_season_index"] == 0.42

    def test_btcusdt_self_ref_without_global(self):
        # Even without _global snapshot, corr/vol_ratio still padded
        r = _mk_redis_with({})
        out = _enrich_crossasset_ctx("BTCUSDT", r)
        assert out["btc_corr_5m"] == 1.0
        assert out["cross_asset_vol_ratio"] == 1.0
        assert "alt_season_index" not in out


# ─── _enrich_sentiment ───────────────────────────────────────────────────────


class TestSentiment:
    def test_fear_greed_normalised_to_unit(self):
        payload = {"value": 75, "ts_ms": _fresh_ts()}
        r = _mk_redis_with({"cache:fear_greed": json.dumps(payload)})
        out = _enrich_sentiment(r)
        assert abs(out["crypto_fear_greed"] - 0.75) < 1e-9

    def test_fear_greed_already_unit_scale(self):
        payload = {"value": 0.5, "ts_ms": _fresh_ts()}
        r = _mk_redis_with({"cache:fear_greed": json.dumps(payload)})
        out = _enrich_sentiment(r)
        assert abs(out["crypto_fear_greed"] - 0.5) < 1e-9

    def test_breadth_emitted(self):
        payload = {"market_breadth_score": 0.4, "ts_ms": _fresh_ts()}
        r = _mk_redis_with({"cache:fear_greed": json.dumps(payload)})
        out = _enrich_sentiment(r)
        assert out["market_breadth_score"] == 0.4

    def test_empty_when_redis_unavailable(self):
        assert _enrich_sentiment(None) == {}


# ─── _enrich_book_features ───────────────────────────────────────────────────


class TestBookFeatures:
    def test_bid_ask_depth_ratio(self):
        out = _enrich_book_features({"depth_bid_5": 100.0, "depth_ask_5": 50.0})
        assert out["bid_ask_depth_ratio"] == 2.0
        assert abs(out["book_imbalance_5lvl"] - (50.0 / 150.0)) < 1e-9

    def test_zero_depths_no_emission(self):
        out = _enrich_book_features({"depth_bid_5": 0, "depth_ask_5": 0})
        assert "bid_ask_depth_ratio" not in out

    def test_depth_pull_ratio(self):
        out = _enrich_book_features({
            "added_bid_rate_ema": 10.0, "added_ask_rate_ema": 10.0,
            "cancel_bid_rate_ema": 8.0, "cancel_ask_rate_ema": 7.0,
        })
        # (8+7) / (10+10) = 0.75
        assert abs(out["depth_pull_ratio"] - 0.75) < 1e-9

    def test_book_refresh_rate(self):
        out = _enrich_book_features({"book_update_rate_ema": 12.5})
        assert out["book_refresh_rate_hz"] == 12.5


# ─── _enrich_microbar ────────────────────────────────────────────────────────


class TestMicrobar:
    def test_body_bps(self):
        out = _enrich_microbar({
            "microbar_open_px": 100.0, "microbar_close_px": 100.1,
        })
        # body = (100.1 - 100) / 100 * 10000 = 10 bps
        assert abs(out["microbar_body_bps"] - 10.0) < 1e-6

    def test_range_bps(self):
        out = _enrich_microbar({
            "microbar_high_px": 101.0, "microbar_low_px": 100.0,
        })
        assert abs(out["microbar_range_bps"] - 100.0) < 1e-6

    def test_vwap_mid_bps(self):
        out = _enrich_microbar({
            "microbar_vwap": 100.5, "microbar_mid_px": 100.0,
        })
        # (100.5 - 100) / 100 * 10000 = 50 bps
        assert abs(out["microbar_vwap_mid_bps"] - 50.0) < 1e-6

    def test_empty_when_no_microbar(self):
        assert _enrich_microbar({}) == {}


# ─── _enrich_momentum ────────────────────────────────────────────────────────


class TestMomentum:
    def test_momentum_10s_logreturn(self):
        import math
        out = _enrich_momentum({"price": 110.0, "price_10s_ago": 100.0})
        expected = math.log(1.1)
        assert abs(out["momentum_10s"] - expected) < 1e-9

    def test_price_to_ema_bps(self):
        out = _enrich_momentum({"price": 101.0, "ema_short": 100.0})
        assert abs(out["price_to_ema_bps"] - 100.0) < 1e-9

    def test_momentum_x_vol_interaction(self):
        import math
        out = _enrich_momentum({
            "price": 110.0, "price_10s_ago": 100.0,
            "vol_ratio": 1.5,
        })
        expected = math.log(1.1) * 1.5
        assert abs(out["momentum_x_vol_ratio"] - expected) < 1e-9

    def test_invalid_zero_prices_no_emission(self):
        out = _enrich_momentum({"price": 0, "price_10s_ago": 100})
        assert "momentum_10s" not in out


# ─── _enrich_vol_features ────────────────────────────────────────────────────


class TestVolFeatures:
    def test_fast_slow_aliases(self):
        out = _enrich_vol_features({"vol_fast": 5.0, "vol_slow": 3.0})
        assert out["vol_fast_bps"] == 5.0
        assert out["vol_slow_bps"] == 3.0

    def test_atr_fallback_aliases(self):
        out = _enrich_vol_features({"atr_fast_bps": 7.0, "atr_slow_bps": 4.0})
        assert out["vol_fast_bps"] == 7.0
        assert out["vol_slow_bps"] == 4.0

    def test_empty_when_no_sources(self):
        assert _enrich_vol_features({}) == {}


# ─── _enrich_execution_stats ─────────────────────────────────────────────────


class TestExecutionStats:
    def test_hgetall_drives_output(self):
        r = MagicMock()
        r.hgetall.return_value = {
            "expectancy_bps": "12.3",
            "profit_factor_roll20": "1.5",
            "kelly_fraction_roll": "0.06",
        }
        out = _enrich_execution_stats("BTCUSDT", r)
        assert out["expectancy_bps"] == 12.3
        assert out["profit_factor_roll20"] == 1.5
        assert out["kelly_fraction_roll"] == 0.06

    def test_empty_hash(self):
        r = MagicMock()
        r.hgetall.return_value = {}
        assert _enrich_execution_stats("BTCUSDT", r) == {}

    def test_redis_error_safe(self):
        r = MagicMock()
        r.hgetall.side_effect = RuntimeError("conn refused")
        assert _enrich_execution_stats("BTCUSDT", r) == {}


# ─── _enrich_liquidation_ctx ─────────────────────────────────────────────────


class TestLiquidationCtx:
    def test_liquidation_usd_1m(self):
        payload = {"liquidation_usd_1m": 5_000_000.0, "ts_ms": _fresh_ts()}
        r = _mk_redis_with({"ctx:liq:BTCUSDT": json.dumps(payload)})
        out = _enrich_liquidation_ctx("BTCUSDT", r)
        assert out["liquidation_usd_1m"] == 5_000_000.0

    def test_alias_liq_usd(self):
        payload = {"liq_usd_1m": 999.0, "ts_ms": _fresh_ts()}
        r = _mk_redis_with({"ctx:liq:BTCUSDT": json.dumps(payload)})
        out = _enrich_liquidation_ctx("BTCUSDT", r)
        assert out["liquidation_usd_1m"] == 999.0


# ─── Top-level enrich_indicators ─────────────────────────────────────────────


class TestEnrichIndicators:
    def test_all_groups_aggregated(self):
        # Build a redis that returns useful data for several keys
        now = _fresh_ts()
        data_map = {
            "ctx:deriv:BTCUSDT": json.dumps({
                "funding_rate": 0.0001, "delta_oi_5m": 100.0, "ts_ms": now,
            }),
            "crossasset:ctx:BTCUSDT": json.dumps({
                "btc_corr_5m": 1.0, "ts_ms": now,
            }),
            "cache:fear_greed": json.dumps({"value": 65, "ts_ms": now}),
            "ctx:liq:BTCUSDT": json.dumps({
                "liquidation_usd_1m": 2_000_000.0, "ts_ms": now,
            }),
        }
        r = MagicMock()
        r.get.side_effect = lambda k: data_map.get(k)
        r.hgetall.return_value = {"expectancy_bps": "8.0"}

        indicators = {
            "depth_bid_5": 100.0, "depth_ask_5": 50.0,
            "microbar_open_px": 100.0, "microbar_close_px": 100.05,
            "price": 100.0, "ema_short": 99.5,
            "vol_fast": 5.0, "vol_slow": 3.0,
        }
        out = enrich_indicators(
            indicators=indicators,
            symbol="BTCUSDT",
            redis_client=r,
        )
        # All groups produced something
        assert "funding_rate_bps" in out
        assert "btc_corr_5m" in out
        assert "crypto_fear_greed" in out
        assert "bid_ask_depth_ratio" in out
        assert "microbar_body_bps" in out
        assert "price_to_ema_bps" in out
        assert "vol_fast_bps" in out
        assert "expectancy_bps" in out
        assert "liquidation_usd_1m" in out

    def test_single_group_failure_isolated(self):
        # If deriv ctx parsing crashes, other groups still emit
        r = MagicMock()
        r.get.side_effect = lambda k: (
            "BROKEN JSON" if k.startswith("ctx:deriv") else None
        )
        r.hgetall.return_value = {}
        out = enrich_indicators(
            indicators={"depth_bid_5": 100, "depth_ask_5": 50},
            symbol="ETHUSDT",
            redis_client=r,
        )
        # Book features should still produce regardless of deriv failure
        assert "bid_ask_depth_ratio" in out
        assert "funding_rate_bps" not in out  # deriv broken

    def test_fail_open_no_redis(self):
        # No Redis client → only in-dict computations work
        indicators = {"depth_bid_5": 100, "depth_ask_5": 50}
        out = enrich_indicators(indicators=indicators, symbol="BTC", redis_client=None)
        assert "bid_ask_depth_ratio" in out
        # Redis-dependent producer groups silent (return {})
        assert "funding_rate_bps" not in out
        # crypto_fear_greed gets stub-defaulted to 0.0 when source absent
        # (final stub-fill keeps v14_of vector shape stable)
        assert out.get("crypto_fear_greed") == 0.0
