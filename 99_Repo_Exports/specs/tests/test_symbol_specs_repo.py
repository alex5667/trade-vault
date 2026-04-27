# -*- coding: utf-8 -*-
"""Tests for specs.symbol_specs_repo."""
import json
import pytest
import fakeredis

from specs.symbol_specs_repo import SymbolSpecsModel, SymbolSpecsRepo


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
FALLBACK = SymbolSpecsModel(
    symbol="BTCUSDT",
    point=0.1,
    tick_value_per_lot=1.0,
)

FULL_PAYLOAD: dict = {
    "point": 0.01,
    "tick_value_per_lot": 2.5,
    "min_lot": 0.001,
    "max_lot": 100.0,
    "lot_step": 0.001,
    "contract_size": 1.0,
    "price_decimals": 2,
    "volume_decimals": 3,
}


# ---------------------------------------------------------------------------
# SymbolSpecsModel.from_dict
# ---------------------------------------------------------------------------
class TestSymbolSpecsModelFromDict:
    def test_full_payload_overrides_fallback(self):
        model = SymbolSpecsModel.from_dict("ETHUSDT", FULL_PAYLOAD, FALLBACK)
        assert model.symbol == "ETHUSDT"
        assert model.point == pytest.approx(0.01)
        assert model.tick_value_per_lot == pytest.approx(2.5)
        assert model.min_lot == pytest.approx(0.001)
        assert model.max_lot == pytest.approx(100.0)
        assert model.price_decimals == 2
        assert model.volume_decimals == 3

    def test_partial_payload_uses_fallback_for_missing_fields(self):
        partial = {"point": 0.05}
        model = SymbolSpecsModel.from_dict("SOLUSDT", partial, FALLBACK)
        assert model.point == pytest.approx(0.05)
        # everything else from fallback
        assert model.tick_value_per_lot == pytest.approx(FALLBACK.tick_value_per_lot)
        assert model.min_lot == pytest.approx(FALLBACK.min_lot)

    def test_empty_dict_returns_all_from_fallback(self):
        model = SymbolSpecsModel.from_dict("XAUUSDT", {}, FALLBACK)
        assert model.symbol == "XAUUSDT"
        assert model.point == pytest.approx(FALLBACK.point)
        assert model.tick_value_per_lot == pytest.approx(FALLBACK.tick_value_per_lot)

    def test_none_values_in_dict_use_fallback(self):
        payload = {"point": None, "tick_value_per_lot": None}
        model = SymbolSpecsModel.from_dict("BNBUSDT", payload, FALLBACK)
        assert model.point == pytest.approx(FALLBACK.point)

    def test_to_dict_roundtrip(self):
        model = SymbolSpecsModel.from_dict("BTCUSDT", FULL_PAYLOAD, FALLBACK)
        d = model.to_dict()
        assert d["point"] == pytest.approx(FULL_PAYLOAD["point"])
        assert d["price_decimals"] == FULL_PAYLOAD["price_decimals"]
        # symbol is NOT in to_dict (it's the Redis key)
        assert "symbol" not in d


# ---------------------------------------------------------------------------
# SymbolSpecsRepo.get
# ---------------------------------------------------------------------------
class TestSymbolSpecsRepoGet:
    def setup_method(self):
        self.r = fakeredis.FakeRedis(decode_responses=False)
        self.repo = SymbolSpecsRepo(self.r)

    def test_missing_key_returns_fallback(self):
        result = self.repo.get("NONEXISTENT", FALLBACK)
        assert result is FALLBACK

    def test_valid_json_returns_model(self):
        self.r.set("symbol_specs:ETHUSDT", json.dumps(FULL_PAYLOAD).encode())
        result = self.repo.get("ETHUSDT", FALLBACK)
        assert result.symbol == "ETHUSDT"
        assert result.point == pytest.approx(0.01)

    def test_corrupted_json_returns_fallback(self):
        self.r.set("symbol_specs:CORRUPT", b"not-json!!!")
        result = self.repo.get("CORRUPT", FALLBACK)
        assert result is FALLBACK

    def test_empty_value_returns_fallback(self):
        self.r.set("symbol_specs:EMPTY", b"")
        result = self.repo.get("EMPTY", FALLBACK)
        assert result is FALLBACK

    def test_custom_key_template(self):
        custom_repo = SymbolSpecsRepo(self.r, key_tpl="specs:{SYMBOL}:v2")
        self.r.set("specs:XAUUSDT:v2", json.dumps({"point": 0.001}).encode())
        result = custom_repo.get("XAUUSDT", FALLBACK)
        assert result.point == pytest.approx(0.001)

    def test_partial_json_uses_fallback_for_missing_fields(self):
        self.r.set("symbol_specs:PARTIAL", json.dumps({"point": 99.0}).encode())
        result = self.repo.get("PARTIAL", FALLBACK)
        assert result.point == pytest.approx(99.0)
        assert result.tick_value_per_lot == pytest.approx(FALLBACK.tick_value_per_lot)


# ---------------------------------------------------------------------------
# SymbolSpecsRepo.upsert
# ---------------------------------------------------------------------------
class TestSymbolSpecsRepoUpsert:
    def setup_method(self):
        self.r = fakeredis.FakeRedis(decode_responses=False)
        self.repo = SymbolSpecsRepo(self.r)

    def test_upsert_without_ttl_persists(self):
        specs = SymbolSpecsModel(
            symbol="BTCUSDT",
            point=0.5,
            tick_value_per_lot=3.0,
        )
        self.repo.upsert(specs)
        raw = self.r.get("symbol_specs:BTCUSDT")
        assert raw is not None
        data = json.loads(raw)
        assert data["point"] == pytest.approx(0.5)

    def test_upsert_with_ttl_persists_and_sets_expiry(self):
        specs = SymbolSpecsModel(
            symbol="ETHUSDT",
            point=0.1,
            tick_value_per_lot=1.0,
        )
        self.repo.upsert(specs, ttl_sec=3600)
        ttl = self.r.ttl("symbol_specs:ETHUSDT")
        assert ttl > 0

    def test_upsert_zero_ttl_no_expiry(self):
        specs = SymbolSpecsModel(
            symbol="SOLUSDT",
            point=0.001,
            tick_value_per_lot=0.001,
        )
        self.repo.upsert(specs, ttl_sec=0)
        ttl = self.r.ttl("symbol_specs:SOLUSDT")
        assert ttl == -1  # no expiry

    def test_get_after_upsert_roundtrip(self):
        specs = SymbolSpecsModel(
            symbol="XAUUSDT",
            point=0.01,
            tick_value_per_lot=0.1,
            min_lot=0.01,
            max_lot=50.0,
        )
        self.repo.upsert(specs)
        result = self.repo.get("XAUUSDT", FALLBACK)
        assert result.symbol == "XAUUSDT"
        assert result.point == pytest.approx(0.01)
        assert result.max_lot == pytest.approx(50.0)
