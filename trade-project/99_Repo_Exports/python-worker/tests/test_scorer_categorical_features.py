"""Tests for the train/serve-symmetric scorer categorical encoders."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from core.scorer_categorical_features import (
    DIRECTION_LONG,
    DIRECTION_SHORT,
    DIRECTION_UNKNOWN,
    REGIME_OTHER,
    REGIME_UNKNOWN,
    SCORER_CATEGORICAL_FEATURES,
    SESSION_OTHER,
    SESSION_UNKNOWN,
    SYMBOL_OTHER,
    SYMBOL_UNKNOWN,
    encode_categorical_from_ctx,
    encode_categorical_from_record,
    encode_direction,
    encode_regime,
    encode_session,
    encode_symbol,
    is_categorical_feature_name,
)


class TestEncodeSymbol:
    def test_known_btc(self):
        assert encode_symbol("BTCUSDT") == 0

    def test_known_eth(self):
        assert encode_symbol("ETHUSDT") == 1

    def test_known_pepe(self):
        assert encode_symbol("1000PEPEUSDT") == 3

    def test_unknown_symbol(self):
        assert encode_symbol("DOGEUSDT") == SYMBOL_OTHER

    def test_lowercase(self):
        assert encode_symbol("btcusdt") == 0  # normalised to upper

    def test_empty(self):
        assert encode_symbol("") == SYMBOL_UNKNOWN

    def test_none(self):
        assert encode_symbol(None) == SYMBOL_UNKNOWN

    def test_placeholder(self):
        assert encode_symbol("?") == SYMBOL_UNKNOWN


class TestEncodeRegime:
    def test_range(self):
        assert encode_regime("range") == 0

    def test_trending_bull(self):
        assert encode_regime("trending_bull") == 1

    def test_uppercase(self):
        assert encode_regime("TRENDING_BEAR") == 2  # normalised to lower

    def test_unknown_explicit(self):
        assert encode_regime("unknown") == REGIME_UNKNOWN

    def test_question_mark(self):
        assert encode_regime("?") == REGIME_UNKNOWN

    def test_na(self):
        assert encode_regime("na") == REGIME_UNKNOWN

    def test_other_named(self):
        assert encode_regime("hyperboost") == REGIME_OTHER


class TestEncodeSession:
    def test_ny(self):
        assert encode_session("NY") == 0

    def test_eu(self):
        assert encode_session("EU") == 1

    def test_london_alias(self):
        # LONDON should normalise to the same idx as EU
        assert encode_session("LONDON") == encode_session("EU") == 1

    def test_asia(self):
        assert encode_session("ASIA") == 2

    def test_off(self):
        assert encode_session("OFF") == 3

    def test_unknown(self):
        assert encode_session("?") == SESSION_UNKNOWN

    def test_other(self):
        assert encode_session("LUNCH") == SESSION_OTHER


class TestEncodeDirection:
    def test_long(self):
        assert encode_direction("LONG") == DIRECTION_LONG
    def test_buy_alias(self):
        assert encode_direction("BUY") == DIRECTION_LONG

    def test_short(self):
        assert encode_direction("SHORT") == DIRECTION_SHORT

    def test_sell_alias(self):
        assert encode_direction("SELL") == DIRECTION_SHORT

    def test_unknown(self):
        assert encode_direction("?") == DIRECTION_UNKNOWN

    def test_empty(self):
        assert encode_direction("") == DIRECTION_UNKNOWN


class TestSymmetricEncoders:
    """Critical contract: encode_from_record(d, inds) and encode_from_ctx(ctx)
    must produce identical output for equivalent inputs."""

    def test_record_returns_all_categorical_keys(self):
        d = {"symbol": "BTCUSDT", "direction": "BUY"}
        inds = {"regime": "trending_bull", "session": "NY"}
        out = encode_categorical_from_record(d, inds)
        assert set(out.keys()) == set(SCORER_CATEGORICAL_FEATURES)

    def test_ctx_returns_all_categorical_keys(self):
        ctx = SimpleNamespace(
            symbol="BTCUSDT", regime_label="trending_bull", regime="trending_bull",
            session="NY", side="LONG",
        )
        out = encode_categorical_from_ctx(ctx)
        assert set(out.keys()) == set(SCORER_CATEGORICAL_FEATURES)

    def test_record_and_ctx_match_for_equivalent_inputs(self):
        d = {"symbol": "ETHUSDT", "direction": "SELL"}
        inds = {"regime": "range", "session": "ASIA"}
        ctx = SimpleNamespace(
            symbol="ETHUSDT", regime_label="range", regime="range",
            session="ASIA", side="SHORT",
        )
        assert encode_categorical_from_record(d, inds) == encode_categorical_from_ctx(ctx)

    def test_record_uses_market_regime_fallback(self):
        d = {"symbol": "BTCUSDT", "direction": "BUY"}
        inds = {"market_regime": "trending_bull", "session": "NY"}  # no "regime"
        out = encode_categorical_from_record(d, inds)
        assert out["_cat_regime_idx"] == 1

    def test_ctx_uses_regime_alias_fallback(self):
        # regime_label empty → falls back to regime
        ctx = SimpleNamespace(
            symbol="BTCUSDT", regime_label="na", regime="trending_bull",
            session="NY", side="LONG",
        )
        out = encode_categorical_from_ctx(ctx)
        assert out["_cat_regime_idx"] == 1

    def test_ctx_uses_side_int_fallback(self):
        ctx = SimpleNamespace(
            symbol="BTCUSDT", regime_label="range", regime="range",
            session="NY", side=None, side_int=1,
        )
        out = encode_categorical_from_ctx(ctx)
        assert out["_cat_direction_idx"] == DIRECTION_LONG

    def test_ctx_unknown_everything(self):
        ctx = SimpleNamespace(symbol="?", regime_label="?", regime="?", session="?", side=None)
        out = encode_categorical_from_ctx(ctx)
        assert out["_cat_symbol_idx"] == SYMBOL_UNKNOWN
        assert out["_cat_regime_idx"] == REGIME_UNKNOWN
        assert out["_cat_session_idx"] == SESSION_UNKNOWN
        assert out["_cat_direction_idx"] == DIRECTION_UNKNOWN


class TestFeatureNamePredicate:
    def test_cat_prefix(self):
        assert is_categorical_feature_name("_cat_symbol_idx")
        assert is_categorical_feature_name("_cat_regime_idx")

    def test_not_cat(self):
        assert not is_categorical_feature_name("delta_z")
        assert not is_categorical_feature_name("symbol_idx")  # missing prefix
