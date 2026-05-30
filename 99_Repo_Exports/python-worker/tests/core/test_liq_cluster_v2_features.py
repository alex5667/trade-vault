"""tests/core/test_liq_cluster_v2_features.py

Tests for Group 1 liq cluster features:
  - liq_cluster_strength_above, liq_cluster_strength_below, liq_cluster_asymmetry
    (computed inside compute_liqmap_features_from_snapshot)
  - liq_absorption_after_sweep_score, liq_sweep_to_cluster_dist_bps
    (computed inside _enrich_liq_cluster_v2 in feature_enricher_v1)
"""
from __future__ import annotations

import json
import time

import pytest

from services.orderflow.liqmap_features import (
    compute_liqmap_features_from_snapshot,
    try_parse_liqmap_snapshot_json,
)
from core.feature_enricher_v1 import _enrich_liq_cluster_v2


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_payload(levels: list[dict], ts_ms: int | None = None) -> dict:
    """Build a liqmap payload dict (parsed via try_parse_liqmap_snapshot_json)."""
    raw = json.dumps({
        "ts_ms": ts_ms if ts_ms is not None else int(time.time() * 1000),
        "levels": levels,
    })
    result = try_parse_liqmap_snapshot_json(raw)
    assert result is not None
    return result


def _call(levels, mid_px=100.0, now_ms=None, max_stale_ms=60_000,
          peak_range_bps=500.0, front_run_bps=5.0, sl_buffer_bps=5.0):
    ts_ms = int(time.time() * 1000) if now_ms is None else now_ms
    payload = _make_payload(levels, ts_ms=ts_ms)
    return compute_liqmap_features_from_snapshot(
        payload=payload,
        mid_px=mid_px,
        now_ms=ts_ms,
        max_stale_ms=max_stale_ms,
        peak_range_bps=peak_range_bps,
        front_run_bps=front_run_bps,
        sl_buffer_bps=sl_buffer_bps,
    )


# ── liq_cluster_strength_above ────────────────────────────────────────────────

class TestLiqClusterStrengthAbove:
    def test_all_shorts_above_mid(self):
        """All short_usd is above mid_px → strength_above ≈ 1.0."""
        levels = [
            {"price": "101.0", "long_usd": "0", "short_usd": "500"},
            {"price": "102.0", "long_usd": "0", "short_usd": "300"},
        ]
        feats = _call(levels, mid_px=100.0)
        assert "liq_cluster_strength_above" in feats
        assert pytest.approx(feats["liq_cluster_strength_above"], abs=1e-6) == 1.0

    def test_no_shorts_above_mid(self):
        """All short_usd is below mid_px → strength_above = 0.0."""
        levels = [
            {"price": "99.0", "long_usd": "0", "short_usd": "400"},
            {"price": "98.0", "long_usd": "0", "short_usd": "200"},
        ]
        feats = _call(levels, mid_px=100.0)
        assert feats["liq_cluster_strength_above"] == 0.0

    def test_partial_above(self):
        """Half of short_usd above mid → strength_above = 0.5."""
        levels = [
            {"price": "101.0", "long_usd": "0", "short_usd": "500"},  # above
            {"price": "99.0",  "long_usd": "0", "short_usd": "500"},  # below
        ]
        feats = _call(levels, mid_px=100.0)
        assert pytest.approx(feats["liq_cluster_strength_above"], abs=1e-6) == 0.5

    def test_no_short_usd_at_all(self):
        """tot_short_usd = 0 → strength_above = 0.0 (no division by zero)."""
        levels = [
            {"price": "101.0", "long_usd": "1000", "short_usd": "0"},
        ]
        feats = _call(levels, mid_px=100.0)
        assert feats["liq_cluster_strength_above"] == 0.0

    def test_value_clamped_to_01(self):
        """Result must be in [0, 1] even with floating-point noise."""
        levels = [
            {"price": "101.0", "long_usd": "0", "short_usd": "1000"},
        ]
        feats = _call(levels, mid_px=100.0)
        val = feats["liq_cluster_strength_above"]
        assert 0.0 <= val <= 1.0


# ── liq_cluster_strength_below ────────────────────────────────────────────────

class TestLiqClusterStrengthBelow:
    def test_all_longs_below_mid(self):
        """All long_usd is below mid_px → strength_below ≈ 1.0."""
        levels = [
            {"price": "99.0", "long_usd": "800", "short_usd": "0"},
            {"price": "98.0", "long_usd": "200", "short_usd": "0"},
        ]
        feats = _call(levels, mid_px=100.0)
        assert pytest.approx(feats["liq_cluster_strength_below"], abs=1e-6) == 1.0

    def test_no_longs_below_mid(self):
        """All long_usd is above mid_px → strength_below = 0.0."""
        levels = [
            {"price": "101.0", "long_usd": "600", "short_usd": "0"},
        ]
        feats = _call(levels, mid_px=100.0)
        assert feats["liq_cluster_strength_below"] == 0.0

    def test_no_long_usd_at_all(self):
        """tot_long_usd = 0 → strength_below = 0.0 (no division by zero)."""
        levels = [
            {"price": "99.0", "long_usd": "0", "short_usd": "500"},
        ]
        feats = _call(levels, mid_px=100.0)
        assert feats["liq_cluster_strength_below"] == 0.0


# ── liq_cluster_asymmetry ─────────────────────────────────────────────────────

class TestLiqClusterAsymmetry:
    def test_symmetric_returns_zero(self):
        """Equal strength above and below → asymmetry ≈ 0.0."""
        levels = [
            {"price": "101.0", "long_usd": "0",   "short_usd": "500"},  # above
            {"price": "99.0",  "long_usd": "500",  "short_usd": "0"},   # below
        ]
        feats = _call(levels, mid_px=100.0)
        # strength_above = 1.0, strength_below = 1.0 → asymmetry = 0
        assert pytest.approx(feats["liq_cluster_asymmetry"], abs=1e-6) == 0.0

    def test_all_above_returns_positive(self):
        """All weight above → asymmetry close to +1."""
        levels = [
            {"price": "101.0", "long_usd": "0", "short_usd": "1000"},
        ]
        feats = _call(levels, mid_px=100.0)
        assert feats["liq_cluster_asymmetry"] > 0.9

    def test_all_below_returns_negative(self):
        """All weight below → asymmetry close to -1."""
        levels = [
            {"price": "99.0", "long_usd": "1000", "short_usd": "0"},
        ]
        feats = _call(levels, mid_px=100.0)
        assert feats["liq_cluster_asymmetry"] < -0.9

    def test_range_minus1_to_1(self):
        """Asymmetry must always be in [-1, 1]."""
        for s_above, l_below in [(0, 0), (100, 0), (0, 200), (300, 150), (50, 50)]:
            levels = []
            if s_above:
                levels.append({"price": "101.0", "long_usd": "0", "short_usd": str(s_above)})
            if l_below:
                levels.append({"price": "99.0", "long_usd": str(l_below), "short_usd": "0"})
            if not levels:
                levels = [{"price": "101.0", "long_usd": "0", "short_usd": "0"}]
            feats = _call(levels, mid_px=100.0)
            val = feats["liq_cluster_asymmetry"]
            assert -1.0 <= val <= 1.0, f"asymmetry={val} out of range for {s_above},{l_below}"

    def test_empty_levels(self):
        """Empty levels → returns default feats dict without crashing."""
        feats = _call([], mid_px=100.0)
        # Empty num_levels: function returns early feats without cluster keys
        # OR returns 0.0 defaults. Either is acceptable as long as no exception.
        assert isinstance(feats, dict)

    def test_no_data_no_exception(self):
        """Bad mid_px → returns empty dict without exception."""
        from services.orderflow.liqmap_features import compute_liqmap_features_from_snapshot
        result = compute_liqmap_features_from_snapshot(
            payload={"ts_ms": 0, "levels": []},
            mid_px=-1.0,  # invalid
            now_ms=0,
            max_stale_ms=60_000,
            peak_range_bps=500.0,
            front_run_bps=5.0,
            sl_buffer_bps=5.0,
        )
        assert result == {}


# ── _enrich_liq_cluster_v2 ────────────────────────────────────────────────────

class _FakeRedis:
    """Minimal fake redis with configurable GET responses."""
    def __init__(self, data: dict[str, str | None] = None):
        self._data = data or {}

    def get(self, key: str):
        return self._data.get(key)

    def mget(self, keys):
        return [self._data.get(k) for k in keys]


@pytest.fixture(autouse=True)
def clear_snapshot_cache():
    """Clear the global enricher snapshot cache between tests."""
    import core.feature_enricher_v1 as fe
    fe._snapshot_cache.clear()
    yield
    fe._snapshot_cache.clear()


class TestEnrichLiqClusterV2:
    def _indicators(self, dist_above=50.0, dist_below=30.0):
        return {
            "liq_cluster_dist_above_bps": dist_above,
            "liq_cluster_dist_below_bps": dist_below,
        }

    def _sweep_payload(self, age_ms=5_000, velocity=60.0, now_ms=None):
        """Build a sweep_v2 JSON payload."""
        _now = now_ms or int(time.time() * 1000)
        return json.dumps({
            "ts_ms": _now - age_ms,
            "sweep_velocity_bps_s": velocity,
        })

    def test_no_sweep_returns_zeros(self):
        r = _FakeRedis({})  # no sweep_v2:BTCUSDT key
        result = _enrich_liq_cluster_v2("BTCUSDT", self._indicators(), r)
        assert result["liq_sweep_to_cluster_dist_bps"] == 0.0
        assert result["liq_absorption_after_sweep_score"] == 0.0

    def test_fresh_sweep_uses_nearest_cluster(self):
        now_ms = int(time.time() * 1000)
        r = _FakeRedis({"sweep_v2:BTCUSDT": self._sweep_payload(age_ms=1_000, velocity=100.0, now_ms=now_ms)})
        ind = self._indicators(dist_above=50.0, dist_below=30.0)
        result = _enrich_liq_cluster_v2("BTCUSDT", ind, r, now_ms=now_ms)
        # nearest cluster = min(50, 30) = 30
        assert result["liq_sweep_to_cluster_dist_bps"] == 30.0

    def test_stale_sweep_returns_zero_dist(self):
        now_ms = int(time.time() * 1000)
        # sweep older than 60s
        r = _FakeRedis({"sweep_v2:BTCUSDT": self._sweep_payload(age_ms=65_000, velocity=50.0, now_ms=now_ms)})
        result = _enrich_liq_cluster_v2("BTCUSDT", self._indicators(), r, now_ms=now_ms)
        assert result["liq_sweep_to_cluster_dist_bps"] == 0.0

    def test_absorption_score_range(self):
        now_ms = int(time.time() * 1000)
        for velocity in (0.0, 25.0, 50.0, 100.0):
            r = _FakeRedis({"sweep_v2:BTCUSDT": self._sweep_payload(age_ms=1_000, velocity=velocity, now_ms=now_ms)})
            result = _enrich_liq_cluster_v2("BTCUSDT", self._indicators(), r, now_ms=now_ms)
            score = result["liq_absorption_after_sweep_score"]
            assert 0.0 <= score <= 1.0, f"score={score} out of range for velocity={velocity}"

    def test_absorption_saturates_at_50_bps_s(self):
        """velocity >= 50 bps/s → score saturates (≈ recency_factor × 1.0)."""
        now_ms = int(time.time() * 1000)
        r_fast = _FakeRedis({"sweep_v2:BTCUSDT": self._sweep_payload(age_ms=1_000, velocity=50.0, now_ms=now_ms)})
        r_vfast = _FakeRedis({"sweep_v2:BTCUSDT": self._sweep_payload(age_ms=1_000, velocity=200.0, now_ms=now_ms)})
        s_fast = _enrich_liq_cluster_v2("BTCUSDT", self._indicators(), r_fast, now_ms=now_ms)["liq_absorption_after_sweep_score"]
        s_vfast = _enrich_liq_cluster_v2("BTCUSDT", self._indicators(), r_vfast, now_ms=now_ms)["liq_absorption_after_sweep_score"]
        assert pytest.approx(s_fast, abs=1e-6) == pytest.approx(s_vfast, abs=1e-6)

    def test_absorption_decays_with_age(self):
        """Older sweep → lower absorption score."""
        import core.feature_enricher_v1 as fe
        now_ms = int(time.time() * 1000)

        fe._snapshot_cache.clear()
        r_new = _FakeRedis({"sweep_v2:BTCUSDT": self._sweep_payload(age_ms=1_000, velocity=50.0, now_ms=now_ms)})
        s_new = _enrich_liq_cluster_v2("BTCUSDT", self._indicators(), r_new, now_ms=now_ms)["liq_absorption_after_sweep_score"]

        fe._snapshot_cache.clear()
        r_old = _FakeRedis({"sweep_v2:BTCUSDT": self._sweep_payload(age_ms=30_000, velocity=50.0, now_ms=now_ms)})
        s_old = _enrich_liq_cluster_v2("BTCUSDT", self._indicators(), r_old, now_ms=now_ms)["liq_absorption_after_sweep_score"]

        assert s_new > s_old

    def test_only_above_cluster(self):
        """Only dist_above populated → sweep_dist = dist_above."""
        now_ms = int(time.time() * 1000)
        r = _FakeRedis({"sweep_v2:BTCUSDT": self._sweep_payload(age_ms=1_000, velocity=50.0, now_ms=now_ms)})
        ind = {"liq_cluster_dist_above_bps": 40.0, "liq_cluster_dist_below_bps": 0.0}
        result = _enrich_liq_cluster_v2("BTCUSDT", ind, r, now_ms=now_ms)
        assert result["liq_sweep_to_cluster_dist_bps"] == 40.0

    def test_only_below_cluster(self):
        """Only dist_below populated → sweep_dist = dist_below."""
        now_ms = int(time.time() * 1000)
        r = _FakeRedis({"sweep_v2:BTCUSDT": self._sweep_payload(age_ms=1_000, velocity=50.0, now_ms=now_ms)})
        ind = {"liq_cluster_dist_above_bps": 0.0, "liq_cluster_dist_below_bps": 25.0}
        result = _enrich_liq_cluster_v2("BTCUSDT", ind, r, now_ms=now_ms)
        assert result["liq_sweep_to_cluster_dist_bps"] == 25.0

    def test_no_redis_returns_zeros(self):
        """redis_client=None → fail-open, return zeros."""
        result = _enrich_liq_cluster_v2("BTCUSDT", self._indicators(), None)
        assert result == {}

    def test_no_symbol_returns_empty(self):
        """Empty symbol → fail-open."""
        r = _FakeRedis({})
        result = _enrich_liq_cluster_v2("", self._indicators(), r)
        assert result == {}
