from __future__ import annotations

from core.book_evidence import compute_obi_flags, compute_iceberg_flags


def test_obi_staleness_and_dir_match():
    indicators = {}
    cfg = {"obi_event_ttl_ms": 5000, "obi_stable_min_secs": 1.5}
    now = 10_000
    last = {"ts_ms": 9_000, "direction": "LONG", "obi": 0.3, "stable_secs": 2.0}
    dir_ok, stable, secs, obi = compute_obi_flags(
        direction="LONG", now_ts_ms=now, last_event=last, cfg=cfg, indicators=indicators
    )
    assert dir_ok is True
    assert stable is True
    assert secs == 2.0
    assert abs(obi - 0.3) < 1e-9


def test_obi_stale_event_is_ignored():
    indicators = {}
    cfg = {"obi_event_ttl_ms": 5000, "obi_stable_min_secs": 1.5}
    now = 20_000
    last = {"ts_ms": 10_000, "direction": "LONG", "obi": 0.3, "stable_secs": 9.0}
    dir_ok, stable, secs, obi = compute_obi_flags(
        direction="LONG", now_ts_ms=now, last_event=last, cfg=cfg, indicators=indicators
    )
    assert dir_ok is False
    assert stable is False


def test_iceberg_strict_with_dist():
    indicators = {}
    cfg = {
        "iceberg_event_ttl_ms": 15000,
        "iceberg_strict_refresh_min": 3,
        "iceberg_strict_duration_min": 1.5,
        "iceberg_strict_dist_bp": 10.0,
    }
    now = 10_000
    last = {"ts_ms": 9_000, "side": "bid", "refresh": 3, "duration": 2.0, "price": 100.00}
    dir_ok, strict, refresh, dur = compute_iceberg_flags(
        direction="LONG", price=100.05, now_ts_ms=now, last_event=last, cfg=cfg, indicators=indicators
    )
    assert dir_ok is True
    assert strict is True
    assert refresh == 3
    assert dur == 2.0


def test_iceberg_stale_is_ignored():
    indicators = {}
    cfg = {"iceberg_event_ttl_ms": 15000}
    now = 40_000
    last = {"ts_ms": 10_000, "side": "bid", "refresh": 999, "duration": 999.0, "price": 100.0}
    dir_ok, strict, refresh, dur = compute_iceberg_flags(
        direction="LONG", price=100.0, now_ts_ms=now, last_event=last, cfg=cfg, indicators=indicators
    )
    assert dir_ok is False
    assert strict is False
