"""Unit tests for _enrich_liqmap_snapshot in feature_enricher_v1.

Verifies that the enricher correctly parses liqmap snapshot `levels` arrays
and produces the `liqmap_{w}_near_short_usd`, `dist_up_bps`, `dist_dn_bps`
keys required by of_confirm_engine Phase 4.7.
"""
import json
import time
from unittest.mock import MagicMock

import pytest

from core.feature_enricher_v1 import _enrich_liqmap_snapshot, _snapshot_cache


def _prime_cache(key: str, data: dict) -> None:
    expire_ns = time.monotonic_ns() + 10_000_000_000  # 10 s
    _snapshot_cache[key] = (data, expire_ns)


def _make_snap(symbol: str = "ETHUSDT", window: str = "1h", price_now: float = 2080.0) -> dict:
    ts_ms = int(time.time() * 1000) - 5000
    return {
        "ts_ms": ts_ms,
        "symbol": symbol,
        "window": window,
        "source_ok": 1,
        "levels": [
            # near level: within 20 bps of 2080 = ±4.16 → 2082 is 1 bps away
            {"price": "2082.0", "long_usd": "1000", "short_usd": "5000", "total_usd": "6000", "bucket": "2082"},
            # far levels above and below for dist computation
            {"price": "2200.0", "long_usd": "2000", "short_usd": "80000", "total_usd": "82000", "bucket": "2200"},
            {"price": "1900.0", "long_usd": "90000", "short_usd": "1000", "total_usd": "91000", "bucket": "1900"},
        ],
    }


def test_produces_dist_up_and_dn_bps() -> None:
    sym, w = "ETHUSDT", "1h"
    _prime_cache(f"liqmap:snapshot:{sym}:{w}", _make_snap(sym, w))
    _prime_cache(f"liqmap:snapshot:{sym}:5m", {})
    r = MagicMock()
    result = _enrich_liqmap_snapshot(sym, r, {"entry": 2080.0})
    assert result.get("liqmap_1h_dist_up_bps", 0.0) > 0.0, "should have dist above"
    assert result.get("liqmap_1h_dist_dn_bps", 0.0) > 0.0, "should have dist below"


def test_near_short_usd_populated_for_close_levels() -> None:
    sym, w = "ETHUSDT", "1h"
    _prime_cache(f"liqmap:snapshot:{sym}:{w}", _make_snap(sym, w))
    _prime_cache(f"liqmap:snapshot:{sym}:5m", {})
    r = MagicMock()
    result = _enrich_liqmap_snapshot(sym, r, {"entry": 2080.0})
    # Level at 2082 is within 20 bps; it has short_usd=5000
    assert result.get("liqmap_1h_near_short_usd", 0.0) > 0.0, "near short_usd from close level"


def test_falls_back_to_1h_when_5m_empty() -> None:
    sym = "ETHUSDT"
    _prime_cache(f"liqmap:snapshot:{sym}:5m", {})  # empty
    _prime_cache(f"liqmap:snapshot:{sym}:1h", _make_snap(sym, "1h"))
    r = MagicMock()
    result = _enrich_liqmap_snapshot(sym, r, {"entry": 2080.0})
    assert "liqmap_1h_dist_up_bps" in result


def test_returns_empty_without_price() -> None:
    sym = "ETHUSDT"
    _prime_cache(f"liqmap:snapshot:{sym}:5m", _make_snap(sym, "5m"))
    r = MagicMock()
    # No price in indicators → no compute
    result = _enrich_liqmap_snapshot(sym, r, {})
    # Function should not crash; without price liqmap_features returns defaults (zeros)
    assert isinstance(result, dict)


def test_returns_empty_for_missing_symbol() -> None:
    r = MagicMock()
    result = _enrich_liqmap_snapshot("", r, {"entry": 100.0})
    assert result == {}


def test_levels_n_matches_snapshot() -> None:
    sym, w = "BTCUSDT", "1h"
    snap = _make_snap(sym, w)
    _prime_cache(f"liqmap:snapshot:{sym}:{w}", snap)
    _prime_cache(f"liqmap:snapshot:{sym}:5m", {})
    r = MagicMock()
    result = _enrich_liqmap_snapshot(sym, r, {"entry": 2080.0})
    assert result.get("liqmap_1h_levels_n", 0.0) == float(len(snap["levels"]))
