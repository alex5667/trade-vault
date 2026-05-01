from __future__ import annotations
"""Tests for p_min map parsing and merging."""

from core.share_map import parse_map, dump_map, merge_updates


def test_pmin_map_roundtrip():
    """Test parsing and dumping p_min maps."""
    m = parse_map('{"BTCUSDT":0.56,"ethusdt":"0.60"}')
    assert m["BTCUSDT"] == 0.56
    assert m["ETHUSDT"] == 0.60
    m2 = merge_updates(m, {"BTCUSDT": 0.58})
    assert m2["BTCUSDT"] == 0.58
    assert m2["ETHUSDT"] == 0.60
    s = dump_map(m2)
    assert "BTCUSDT" in s
    assert "ETHUSDT" in s


def test_pmin_map_empty():
    """Test empty map handling."""
    m = parse_map("")
    assert m == {}
    m = parse_map("{}")
    assert m == {}


def test_pmin_map_invalid():
    """Test invalid JSON handling."""
    m = parse_map("not json")
    assert m == {}
    m = parse_map('{"invalid": "not a number"}')
    assert m == {}


def test_pmin_map_merge():
    """Test merging updates into base map."""
    base = {"BTCUSDT": 0.55, "ETHUSDT": 0.60}
    updates = {"BTCUSDT": 0.58, "SOLUSDT": 0.62}
    merged = merge_updates(base, updates)
    assert merged["BTCUSDT"] == 0.58  # updated
    assert merged["ETHUSDT"] == 0.60  # preserved
    assert merged["SOLUSDT"] == 0.62  # added

