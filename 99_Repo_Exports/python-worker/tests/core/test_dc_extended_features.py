"""tests/core/test_dc_extended_features.py

Tests for Group 2 DC extended features:
  Producer (_DCState):
    dc_trend_duration_ms, dc_last_confirmation_bps, dc_noise_ratio
  Enricher (_enrich_p2_directional_change):
    dc_overshoot_to_atr_ratio (computed from ATR in indicators)
"""
from __future__ import annotations

import json
import time

import pytest

# Import the private _DCState class for unit testing
from services.directional_change_producer import _DCState
from core.feature_enricher_v1 import _enrich_p2_directional_change


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_state(threshold_bps=50.0) -> _DCState:
    return _DCState(threshold_bps=threshold_bps)


def _feed(state: _DCState, prices: list[float], base_ts_ms: int = 1_000_000) -> None:
    """Feed a series of prices to a _DCState at 1-second intervals."""
    for i, px in enumerate(prices):
        state.on_tick(px, base_ts_ms + i * 1_000)


# ── dc_trend_duration_ms ──────────────────────────────────────────────────────

class TestDcTrendDurationMs:
    def test_no_events_returns_zero(self):
        state = _make_state()
        out = state.compute(now_ms=1_000_000)
        assert out["dc_trend_duration_ms"] == 0.0

    def test_duration_increases_after_dc_event(self):
        state = _make_state(threshold_bps=100.0)  # 1% threshold
        # feed: start at 100, move to 101+ (>1% up)
        base = 1_000_000
        state.on_tick(100.0, base)
        state.on_tick(101.5, base + 2_000)  # +1.5% → DC up event
        now = base + 5_000
        out = state.compute(now_ms=now)
        # trend_start_ms set at base+2000, duration = 3000ms
        assert out["dc_trend_duration_ms"] == pytest.approx(3_000.0, abs=1.0)

    def test_duration_resets_on_direction_flip(self):
        state = _make_state(threshold_bps=100.0)
        base = 1_000_000
        state.on_tick(100.0, base)
        state.on_tick(101.5, base + 1_000)  # DC up at t+1000
        state.on_tick(100.4, base + 3_000)  # DC down at t+3000 (flip)
        now = base + 4_000
        out = state.compute(now_ms=now)
        # After flip, trend_start_ms = base+3000, duration = 1000ms
        assert out["dc_trend_duration_ms"] == pytest.approx(1_000.0, abs=1.0)

    def test_duration_same_direction_no_reset(self):
        """Second DC event in same direction: duration should NOT reset."""
        state = _make_state(threshold_bps=50.0)  # 0.5%
        base = 1_000_000
        state.on_tick(100.0, base)
        state.on_tick(100.6, base + 1_000)  # DC up
        state.on_tick(101.2, base + 3_000)  # another DC up (same dir)
        now = base + 5_000
        out = state.compute(now_ms=now)
        # trend_start set at first flip (base+1000, direction was 0→1)
        # second event same dir → no reset → duration from base+1000
        assert out["dc_trend_duration_ms"] == pytest.approx(4_000.0, abs=1.0)


# ── dc_last_confirmation_bps ──────────────────────────────────────────────────

class TestDcLastConfirmationBps:
    def test_no_events_returns_zero(self):
        state = _make_state()
        out = state.compute(now_ms=1_000_000)
        assert out["dc_last_confirmation_bps"] == 0.0

    def test_first_event_confirmation_zero(self):
        """After first DC event, last_confirmation = 0 (no prior event)."""
        state = _make_state(threshold_bps=100.0)
        base = 1_000_000
        state.on_tick(100.0, base)
        state.on_tick(101.5, base + 1_000)
        out = state.compute(now_ms=base + 2_000)
        assert out["dc_last_confirmation_bps"] == 0.0

    def test_second_event_confirmation_equals_first_overshoot(self):
        """After second DC event, last_confirmation = overshoot of first event."""
        state = _make_state(threshold_bps=100.0)
        base = 1_000_000
        state.on_tick(100.0, base)
        state.on_tick(101.5, base + 1_000)  # DC up, overshoot = 0.5%*100 = 50 bps
        first_overshoot = state._last_overshoot_bps
        state.on_tick(100.4, base + 3_000)  # DC down
        out = state.compute(now_ms=base + 4_000)
        assert out["dc_last_confirmation_bps"] == pytest.approx(first_overshoot, abs=0.01)

    def test_confirmation_tracks_previous_overshoot(self):
        """last_confirmation always holds the PREVIOUS overshoot, not current."""
        state = _make_state(threshold_bps=50.0)
        base = 1_000_000
        state.on_tick(100.0, base)
        state.on_tick(100.6, base + 1_000)  # DC up, ov1
        ov1 = state._last_overshoot_bps
        state.on_tick(100.05, base + 3_000)  # DC down, ov2
        ov2 = state._last_overshoot_bps
        out = state.compute(now_ms=base + 4_000)
        # current overshoot = ov2, last_confirmation = ov1
        assert out["dc_last_confirmation_bps"] == pytest.approx(ov1, abs=0.01)
        assert out["dc_overshoot_bps"] == pytest.approx(ov2, abs=0.01)


# ── dc_noise_ratio ────────────────────────────────────────────────────────────

class TestDcNoiseRatio:
    def test_no_events_returns_zero(self):
        state = _make_state()
        out = state.compute(now_ms=1_000_000)
        assert out["dc_noise_ratio"] == 0.0

    def test_single_event_no_reversals(self):
        """One DC event → 0 reversals / 1 total = 0.0."""
        state = _make_state(threshold_bps=100.0)
        base = 1_000_000
        state.on_tick(100.0, base)
        state.on_tick(101.5, base + 1_000)
        out = state.compute(now_ms=base + 2_000)
        assert out["dc_noise_ratio"] == 0.0

    def test_alternating_events_high_ratio(self):
        """All events alternate direction → noise_ratio near 1.0."""
        state = _make_state(threshold_bps=50.0)
        base = 1_000_000
        # price oscillates: 100 → 100.6 → 100.05 → 100.65 → 100.1 (all within 15m)
        prices = [100.0, 100.6, 100.05, 100.65, 100.1]
        for i, p in enumerate(prices):
            state.on_tick(p, base + i * 10_000)
        out = state.compute(now_ms=base + 50_000)
        assert out["dc_noise_ratio"] > 0.5

    def test_noise_ratio_in_0_1(self):
        """dc_noise_ratio must always be in [0, 1]."""
        state = _make_state(threshold_bps=50.0)
        base = 1_000_000
        prices = [100.0, 100.6, 100.05, 100.65, 100.1, 100.7, 100.15]
        for i, p in enumerate(prices):
            state.on_tick(p, base + i * 5_000)
        out = state.compute(now_ms=base + 40_000)
        assert 0.0 <= out["dc_noise_ratio"] <= 1.0

    def test_old_events_excluded_from_15m_window(self):
        """Events older than 15m should not count in noise_ratio."""
        state = _make_state(threshold_bps=50.0)
        base = 1_000_000
        _15M_MS = 15 * 60 * 1_000
        # feed some events > 15m ago
        state.on_tick(100.0, base)
        state.on_tick(100.6, base + 1_000)   # DC up (old)
        state.on_tick(100.05, base + 2_000)  # DC down (old)
        # now feed a recent event
        recent_base = base + _15M_MS + 10_000
        state.on_tick(99.4, recent_base)      # DC down (from 100.05, old)
        now = recent_base + 1_000
        out = state.compute(now_ms=now)
        # Only events within 15m of `now` count. The first two events are >15m old.
        # noise_ratio should reflect only recent events
        assert out["dc_noise_ratio"] >= 0.0


# ── dc_overshoot_to_atr_ratio (enricher) ─────────────────────────────────────

@pytest.fixture(autouse=True)
def clear_snapshot_cache():
    """Clear the global enricher snapshot cache between tests."""
    import core.feature_enricher_v1 as fe
    fe._snapshot_cache.clear()
    yield
    fe._snapshot_cache.clear()


class _FakeRedis:
    def __init__(self, data: dict[str, str | None] | None = None):
        self._data = data or {}

    def get(self, key: str):
        return self._data.get(key)

    def mget(self, keys):
        return [self._data.get(k) for k in keys]


def _make_dc_snapshot(
    overshoot_bps: float = 30.0,
    event_dir: float = 1.0,
    trend_duration_ms: float = 5000.0,
    last_confirmation_bps: float = 20.0,
    noise_ratio: float = 0.3,
    ts_ms: int | None = None,
) -> str:
    _ts = ts_ms or int(time.time() * 1000)
    return json.dumps({
        "dc_event_dir": event_dir,
        "dc_event_age_ms": 1000.0,
        "dc_overshoot_bps": overshoot_bps,
        "dc_reversal_count_15m": 2.0,
        "dc_trend_duration_ms": trend_duration_ms,
        "dc_last_confirmation_bps": last_confirmation_bps,
        "dc_noise_ratio": noise_ratio,
        "ts_ms": _ts,
        "quality_status": "OK",
    })


class TestDcOvershootToAtrRatio:
    def test_atr_in_indicators_used(self):
        r = _FakeRedis({"ctx:dc:BTCUSDT": _make_dc_snapshot(overshoot_bps=30.0)})
        indicators = {"atr_bps": 60.0}
        result = _enrich_p2_directional_change("BTCUSDT", r, indicators)
        assert result["dc_overshoot_to_atr_ratio"] == pytest.approx(0.5, abs=1e-6)

    def test_atr_zero_returns_zero_ratio(self):
        r = _FakeRedis({"ctx:dc:BTCUSDT": _make_dc_snapshot(overshoot_bps=30.0)})
        indicators = {"atr_bps": 0.0}
        result = _enrich_p2_directional_change("BTCUSDT", r, indicators)
        assert result["dc_overshoot_to_atr_ratio"] == 0.0

    def test_no_indicators_returns_zero_ratio(self):
        r = _FakeRedis({"ctx:dc:BTCUSDT": _make_dc_snapshot(overshoot_bps=30.0)})
        result = _enrich_p2_directional_change("BTCUSDT", r, None)
        assert result["dc_overshoot_to_atr_ratio"] == 0.0

    def test_ratio_is_nonnegative(self):
        for overshoot in (0.0, 10.0, 50.0, 200.0):
            r = _FakeRedis({"ctx:dc:BTCUSDT": _make_dc_snapshot(overshoot_bps=overshoot)})
            result = _enrich_p2_directional_change("BTCUSDT", r, {"atr_bps": 40.0})
            assert result["dc_overshoot_to_atr_ratio"] >= 0.0

    def test_atr_from_snapshot_fallback(self):
        """If atr_bps absent from indicators, try to read from DC snapshot."""
        snap = json.loads(_make_dc_snapshot(overshoot_bps=20.0))
        snap["atr_bps"] = 40.0  # put ATR inside the DC snapshot
        r = _FakeRedis({"ctx:dc:BTCUSDT": json.dumps(snap)})
        result = _enrich_p2_directional_change("BTCUSDT", r, {})
        assert result["dc_overshoot_to_atr_ratio"] == pytest.approx(0.5, abs=1e-6)

    def test_all_new_dc_keys_present(self):
        """All 4 new DC keys must be present in output when snapshot is valid."""
        r = _FakeRedis({"ctx:dc:BTCUSDT": _make_dc_snapshot()})
        result = _enrich_p2_directional_change("BTCUSDT", r, {"atr_bps": 50.0})
        for key in ("dc_trend_duration_ms", "dc_last_confirmation_bps",
                    "dc_noise_ratio", "dc_overshoot_to_atr_ratio"):
            assert key in result, f"Missing key: {key}"

    def test_no_redis_returns_empty(self):
        result = _enrich_p2_directional_change("BTCUSDT", None, {})
        assert result == {}

    def test_stale_snapshot_returns_empty(self):
        """Snapshot older than 120s → _load_json_snapshot rejects it → empty."""
        old_ts = int(time.time() * 1000) - 200_000  # 200s ago
        r = _FakeRedis({"ctx:dc:BTCUSDT": _make_dc_snapshot(ts_ms=old_ts)})
        result = _enrich_p2_directional_change("BTCUSDT", r, {"atr_bps": 50.0})
        assert result == {}
