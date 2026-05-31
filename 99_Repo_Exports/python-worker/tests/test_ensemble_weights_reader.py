"""Tests for services.ensemble_weights_reader."""
from __future__ import annotations

import math
from unittest.mock import MagicMock

import pytest

from services.ensemble_weights_reader import (
    EnsembleWeightsReader,
    _equal_weight_blend,
)


@pytest.fixture(autouse=True)
def _reset_env(monkeypatch):
    monkeypatch.delenv("ENSEMBLE_WEIGHTS_READ_ENABLED", raising=False)
    yield


def test_disabled_returns_none_for_lookup():
    rc = MagicMock()
    rc.hgetall.return_value = {"a": "0.6", "b": "0.4"}
    r = EnsembleWeightsReader(rc, ttl_sec=10.0)
    assert r.lookup_source_weight("BTCUSDT", "a") is None


def test_enabled_returns_weight(monkeypatch):
    monkeypatch.setenv("ENSEMBLE_WEIGHTS_READ_ENABLED", "1")
    rc = MagicMock()
    rc.hgetall.return_value = {"a": "0.6", "b": "0.4"}
    r = EnsembleWeightsReader(rc, ttl_sec=10.0)
    assert math.isclose(r.lookup_source_weight("BTCUSDT", "a"), 0.6)


def test_lookup_unknown_source_returns_none(monkeypatch):
    monkeypatch.setenv("ENSEMBLE_WEIGHTS_READ_ENABLED", "1")
    rc = MagicMock()
    rc.hgetall.return_value = {"a": "0.6"}
    r = EnsembleWeightsReader(rc, ttl_sec=10.0)
    assert r.lookup_source_weight("BTCUSDT", "missing") is None


def test_blend_disabled_falls_back_to_equal_weight(monkeypatch):
    rc = MagicMock()
    rc.hgetall.return_value = {"a": "0.9", "b": "0.1"}  # would skew if used
    r = EnsembleWeightsReader(rc, ttl_sec=10.0)
    # Equal-weight blend of p=0.8 + p=0.2 in logit space → 0.5
    blended = r.blend("BTCUSDT", {"a": 0.8, "b": 0.2})
    assert math.isclose(blended, 0.5, abs_tol=1e-6)


def test_blend_enabled_concentrates_on_higher_weight(monkeypatch):
    monkeypatch.setenv("ENSEMBLE_WEIGHTS_READ_ENABLED", "1")
    rc = MagicMock()
    rc.hgetall.return_value = {"a": "0.9", "b": "0.1"}
    r = EnsembleWeightsReader(rc, ttl_sec=10.0)
    blended = r.blend("BTCUSDT", {"a": 0.8, "b": 0.2})
    # Closer to 0.8 than 0.5
    assert blended > 0.6


def test_blend_falls_back_when_symbol_absent(monkeypatch):
    monkeypatch.setenv("ENSEMBLE_WEIGHTS_READ_ENABLED", "1")
    rc = MagicMock()
    rc.hgetall.return_value = {}  # no weights for symbol
    r = EnsembleWeightsReader(rc, ttl_sec=10.0)
    assert math.isclose(r.blend("BTCUSDT", {"a": 0.6, "b": 0.6}), 0.6, abs_tol=1e-6)


def test_blend_handles_empty_probs(monkeypatch):
    monkeypatch.setenv("ENSEMBLE_WEIGHTS_READ_ENABLED", "1")
    rc = MagicMock()
    rc.hgetall.return_value = {"a": "1.0"}
    r = EnsembleWeightsReader(rc, ttl_sec=10.0)
    assert r.blend("BTCUSDT", {}) == 0.5


def test_blend_ignores_zero_weight_sources(monkeypatch):
    monkeypatch.setenv("ENSEMBLE_WEIGHTS_READ_ENABLED", "1")
    rc = MagicMock()
    rc.hgetall.return_value = {"a": "1.0", "b": "0.0"}
    r = EnsembleWeightsReader(rc, ttl_sec=10.0)
    # Only "a" contributes → blend == p_a
    assert math.isclose(r.blend("BTCUSDT", {"a": 0.7, "b": 0.1}), 0.7, abs_tol=1e-6)


def test_blend_falls_back_when_no_source_has_weight(monkeypatch):
    monkeypatch.setenv("ENSEMBLE_WEIGHTS_READ_ENABLED", "1")
    rc = MagicMock()
    rc.hgetall.return_value = {"x": "1.0"}  # weight for unrelated source
    r = EnsembleWeightsReader(rc, ttl_sec=10.0)
    # No source overlap → equal-weight fallback
    assert math.isclose(r.blend("BTC", {"a": 0.7, "b": 0.3}), 0.5, abs_tol=1e-6)


def test_blend_uses_ttl_cache(monkeypatch):
    monkeypatch.setenv("ENSEMBLE_WEIGHTS_READ_ENABLED", "1")
    rc = MagicMock()
    rc.hgetall.return_value = {"a": "1.0"}
    r = EnsembleWeightsReader(rc, ttl_sec=10.0)
    r.blend("BTCUSDT", {"a": 0.6})
    r.blend("BTCUSDT", {"a": 0.6})
    assert rc.hgetall.call_count == 1


def test_equal_weight_blend_basic():
    # logit(0.7)=0.847; logit(0.3)=-0.847; mean=0 → 0.5
    assert math.isclose(_equal_weight_blend({"a": 0.7, "b": 0.3}), 0.5, abs_tol=1e-6)
