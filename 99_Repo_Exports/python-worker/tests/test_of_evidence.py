from __future__ import annotations

from types import SimpleNamespace

from core.of_evidence import compute_sweep_recent, compute_reclaim_recent, compute_absorption_flags, _get


def test_sweep_recent_stale_false():
    cfg = {"sweep_valid_ms": 120_000}
    indicators = {}
    sw = SimpleNamespace(ts_ms=0, kind="EQH", direction_bias="SHORT")
    assert compute_sweep_recent(now_ts_ms=10_000, last_sweep=sw, cfg=cfg, indicators=indicators) is False


def test_sweep_recent_true():
    cfg = {"sweep_valid_ms": 120_000}
    indicators = {}
    sw = SimpleNamespace(ts_ms=9_500, kind="EQH", direction_bias="SHORT")
    assert compute_sweep_recent(now_ts_ms=10_000, last_sweep=sw, cfg=cfg, indicators=indicators) is True
    assert indicators["sweep_age_ms"] == 500


def test_reclaim_recent_direction_match():
    cfg = {"reclaim_signal_valid_ms": 120_000}
    indicators = {}
    ev = SimpleNamespace(ts_ms=9_000, hold_bars=2, direction_bias="LONG", level=100.0, pool_id="p1")
    ok, bars = compute_reclaim_recent(direction="LONG", now_ts_ms=10_000, last_reclaim=ev, cfg=cfg, indicators=indicators)
    assert ok is True
    assert bars == 2
    assert indicators["reclaim_age_ms"] == 1000


def test_reclaim_recent_wrong_direction_false():
    cfg = {"reclaim_signal_valid_ms": 120_000}
    indicators = {}
    ev = SimpleNamespace(ts_ms=9_000, hold_bars=2, direction_bias="SHORT")
    ok, bars = compute_reclaim_recent(direction="LONG", now_ts_ms=10_000, last_reclaim=ev, cfg=cfg, indicators=indicators)
    assert ok is False
    assert bars == 0


def test_absorption_flags_threshold():
    cfg = {"absorption_min_volume": 10.0}
    indicators = {}
    absorption = {"side": "LONG", "volume": 9.0}
    ok, vol = compute_absorption_flags(direction="LONG", absorption=absorption, cfg=cfg, indicators=indicators)
    assert ok is False
    assert vol == 9.0

    indicators2 = {}
    absorption2 = {"side": "LONG", "volume": 12.0}
    ok2, vol2 = compute_absorption_flags(direction="LONG", absorption=absorption2, cfg=cfg, indicators=indicators2)
    assert ok2 is True
    assert vol2 == 12.0


def test_get_helper_dict_access():
    """Test _get helper with dict access (replay-friendly)."""
    obj = {"ts_ms": 1000, "kind": "EQH", "direction_bias": "LONG"}
    assert _get(obj, "ts_ms", 0) == 1000
    assert _get(obj, "kind", "") == "EQH"
    assert _get(obj, "missing", "default") == "default"
    assert _get(obj, "missing", None) is None


def test_get_helper_object_access():
    """Test _get helper with object access (backward compatible)."""
    obj = SimpleNamespace(ts_ms=2000, kind="EQL", direction_bias="SHORT")
    assert _get(obj, "ts_ms", 0) == 2000
    assert _get(obj, "kind", "") == "EQL"
    assert _get(obj, "missing", "default") == "default"


def test_get_helper_none():
    """Test _get helper with None object."""
    assert _get(None, "any", "default") == "default"
    assert _get(None, "any", None) is None


def test_sweep_recent_with_dict():
    """Test compute_sweep_recent with dict last_sweep (replay-friendly)."""
    cfg = {"sweep_valid_ms": 120_000}
    indicators = {}
    sw = {"ts_ms": 9_500, "kind": "EQH", "direction_bias": "SHORT"}
    assert compute_sweep_recent(now_ts_ms=10_000, last_sweep=sw, cfg=cfg, indicators=indicators) is True
    assert indicators["sweep_age_ms"] == 500
    assert indicators["sweep_kind"] == "EQH"
    assert indicators["sweep_dir_bias"] == "SHORT"


def test_reclaim_recent_with_dict():
    """Test compute_reclaim_recent with dict last_reclaim (replay-friendly)."""
    cfg = {"reclaim_signal_valid_ms": 120_000}
    indicators = {}
    ev = {"ts_ms": 9_000, "hold_bars": 2, "direction_bias": "LONG", "level": 100.0, "pool_id": "p1"}
    ok, bars = compute_reclaim_recent(direction="LONG", now_ts_ms=10_000, last_reclaim=ev, cfg=cfg, indicators=indicators)
    assert ok is True
    assert bars == 2
    assert indicators["reclaim_age_ms"] == 1000
    assert indicators["reclaim_level"] == 100.0
    assert indicators["reclaim_pool_id"] == "p1"
