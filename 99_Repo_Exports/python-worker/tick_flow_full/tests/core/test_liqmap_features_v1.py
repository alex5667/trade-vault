import json
import pytest

from core.liqmap_features_v1 import (
    compute_liqmap_features,
    make_liqmap_default_features,
    parse_liqmap_snapshot_v1,
)


def test_liqmap_parse_and_compute_basic():
    # Snapshot intentionally misses `window` in payload; runtime supplies expected window.
    raw = json.dumps(
        {
            "ts_ms": 1000,
            "symbol": "btcusdt",
            "levels": [
                {"price": 99.0, "long_usd": 100.0, "short_usd": 400.0},
                {"price": 100.0, "long_usd": 120.0, "short_usd": 80.0},
                {"price": 101.0, "long_usd": 900.0, "short_usd": 100.0},
            ],
        }
    )

    snap = parse_liqmap_snapshot_v1(raw, expected_symbol="BTCUSDT", expected_window="5m")
    assert snap.symbol == "BTCUSDT"
    assert snap.window == "5m"

    feats = compute_liqmap_features(
        snap,
        price=100.0,
        windows=("5m",),
        near_band_bps=20.0,  # +/-0.2
        peak_min_share=0.05,
        now_ms=2000,
    )

    # Stable key set
    defaults = make_liqmap_default_features(["5m"])
    assert set(defaults.keys()) <= set(feats.keys())

    assert feats["liqmap_5m_age_ms"] == 1000.0
    assert feats["liqmap_5m_levels_n"] == 3.0

    # Near band contains only price==100.0 level in this synthetic snapshot
    assert feats["liqmap_5m_near_total_usd"] == pytest.approx(200.0)

    # Closest peaks above/below (threshold is small here; both sides qualify)
    assert feats["liqmap_5m_dist_up_bps"] == pytest.approx(100.0)
    assert feats["liqmap_5m_dist_dn_bps"] == pytest.approx(100.0)
    assert feats["liqmap_5m_peak_up1_usd"] == pytest.approx(1000.0)
    assert feats["liqmap_5m_peak_dn1_usd"] == pytest.approx(500.0)


def test_liqmap_default_features_all_zero():
    defaults = make_liqmap_default_features(["5m", "1h"])
    for v in defaults.values():
        assert v == 0.0, f"Expected 0.0 but got {v}"


def test_liqmap_parse_forces_expected_window():
    raw = json.dumps({
        "ts_ms": 5000,
        "symbol": "ETHUSDT",
        "window": "4h",  # payload says 4h but we expect 1h
        "levels": [],
    })
    snap = parse_liqmap_snapshot_v1(raw, expected_symbol="ETHUSDT", expected_window="1h")
    assert snap.window == "1h"  # forced to expected


def test_liqmap_parse_empty_levels():
    raw = json.dumps({"ts_ms": 3000, "symbol": "SOLUSDT", "levels": []})
    snap = parse_liqmap_snapshot_v1(raw, expected_symbol="SOLUSDT", expected_window="5m")
    feats = compute_liqmap_features(snap, price=50.0, windows=("5m",), near_band_bps=20.0, peak_min_share=0.05, now_ms=5000)
    assert feats["liqmap_5m_levels_n"] == 0.0
    assert feats["liqmap_5m_near_total_usd"] == 0.0


def test_liqmap_parse_invalid_json_raises():
    with pytest.raises(ValueError):
        parse_liqmap_snapshot_v1("not json at all", expected_symbol="BTCUSDT", expected_window="5m")
