from __future__ import annotations

from core.share_map import parse_map, dump_map, clamp_map, merge_updates


def test_parse_dump_clamp():
    """Test parse_map, dump_map, clamp_map."""
    m = parse_map('{"btcusdt":0.1,"ETHUSDT":"0.2"}')
    assert m["BTCUSDT"] == 0.1
    assert m["ETHUSDT"] == 0.2
    s = dump_map(m)
    assert "BTCUSDT" in s and "ETHUSDT" in s
    cm = clamp_map(m, 0.05)
    assert cm["BTCUSDT"] == 0.05
    assert cm["ETHUSDT"] == 0.05


def test_merge_updates():
    """Test merge_updates."""
    base = {"BTCUSDT": 0.1, "ETHUSDT": 0.2}
    updates = {"BTCUSDT": 0.15, "SOLUSDT": 0.05}
    merged = merge_updates(base, updates)
    assert merged["BTCUSDT"] == 0.15
    assert merged["ETHUSDT"] == 0.2
    assert merged["SOLUSDT"] == 0.05

