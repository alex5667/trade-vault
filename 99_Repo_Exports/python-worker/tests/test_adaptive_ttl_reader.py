"""Tests for services.adaptive_ttl_reader."""
from __future__ import annotations

import json
import os
from unittest.mock import MagicMock

import pytest

from services.adaptive_ttl_reader import AdaptiveTTLReader


_PAYLOAD = json.dumps(
    dict(
        v=1,
        generated_at_ms=1_780_000_000_000,
        n=2,
        recs=[
            dict(
                symbol="BTCUSDT", regime="momentum", direction=1,
                n=60, win_rate=0.55, tp_r=1.3, sl_r=0.8,
                median_mfe_r=1.3, p10_mae_r=-0.8,
            ),
            dict(
                symbol="ETHUSDT", regime="ranging", direction=-1,
                n=80, win_rate=0.6, tp_r=0.9, sl_r=0.6,
                median_mfe_r=0.9, p10_mae_r=-0.6,
            ),
        ],
    )
)


@pytest.fixture(autouse=True)
def _reset_env(monkeypatch):
    monkeypatch.delenv("ADAPTIVE_TTL_READ_ENABLED", raising=False)
    yield


def test_reader_disabled_by_default(monkeypatch):
    rc = MagicMock()
    rc.get.return_value = _PAYLOAD
    r = AdaptiveTTLReader(rc, ttl_sec=10.0)
    assert r.lookup("BTCUSDT", "momentum", 1) is None


def test_reader_returns_match_when_enabled(monkeypatch):
    monkeypatch.setenv("ADAPTIVE_TTL_READ_ENABLED", "1")
    rc = MagicMock()
    rc.get.return_value = _PAYLOAD
    r = AdaptiveTTLReader(rc, ttl_sec=10.0)
    hit = r.lookup("BTCUSDT", "momentum", 1)
    assert hit is not None
    assert hit["tp_r"] == 1.3
    assert hit["sl_r"] == 0.8


def test_reader_returns_none_on_miss(monkeypatch):
    monkeypatch.setenv("ADAPTIVE_TTL_READ_ENABLED", "1")
    rc = MagicMock()
    rc.get.return_value = _PAYLOAD
    r = AdaptiveTTLReader(rc, ttl_sec=10.0)
    assert r.lookup("BTCUSDT", "ranging", 1) is None  # group doesn't exist


def test_reader_handles_missing_snapshot(monkeypatch):
    monkeypatch.setenv("ADAPTIVE_TTL_READ_ENABLED", "1")
    rc = MagicMock()
    rc.get.return_value = None
    r = AdaptiveTTLReader(rc, ttl_sec=10.0)
    assert r.lookup("BTCUSDT", "momentum", 1) is None


def test_reader_handles_redis_error(monkeypatch):
    monkeypatch.setenv("ADAPTIVE_TTL_READ_ENABLED", "1")
    rc = MagicMock()
    rc.get.side_effect = Exception("redis down")
    r = AdaptiveTTLReader(rc, ttl_sec=10.0)
    # First call → load fails → empty cache → None
    assert r.lookup("BTCUSDT", "momentum", 1) is None


def test_reader_cache_respects_ttl(monkeypatch):
    monkeypatch.setenv("ADAPTIVE_TTL_READ_ENABLED", "1")
    rc = MagicMock()
    rc.get.return_value = _PAYLOAD
    r = AdaptiveTTLReader(rc, ttl_sec=10.0)
    assert r.lookup("BTCUSDT", "momentum", 1) is not None
    assert r.lookup("BTCUSDT", "momentum", 1) is not None
    # Long TTL → only loaded once
    assert rc.get.call_count == 1


def test_reader_normalizes_case(monkeypatch):
    monkeypatch.setenv("ADAPTIVE_TTL_READ_ENABLED", "1")
    rc = MagicMock()
    rc.get.return_value = _PAYLOAD
    r = AdaptiveTTLReader(rc, ttl_sec=10.0)
    assert r.lookup("btcusdt", "MOMENTUM", 1) is not None


def test_reader_size_reflects_cache(monkeypatch):
    monkeypatch.setenv("ADAPTIVE_TTL_READ_ENABLED", "1")
    rc = MagicMock()
    rc.get.return_value = _PAYLOAD
    r = AdaptiveTTLReader(rc, ttl_sec=10.0)
    r.lookup("BTCUSDT", "momentum", 1)
    assert r.size() == 2
