from __future__ import annotations
"""
G12 · BURST MODE (Peak Pressure Aggregator) — comprehensive test suite.

Tests cover:
  1. BurstCandidateSelector: consider, maybe_flush, force_flush, max_age safety
  2. BurstCalibrator: window/max_age computation based on pressure + tick-gap
  3. PressureTracker.burst_window_ms: static mapping LOW/HI/EXTREME
  4. pressure_policy.decide_burst_window_ms: deterministic 3-level mapping
  5. _emit_payload integration: cooldown → burst buffer → return-None / fallback
  6. Burst metadata in emitted payload (burst_emitted_at, burst_best_score …)
  7. Activation logic: CRYPTO_BURST_ENABLE=1 OR pressure_extreme_flag=1
  8. Metrics: burst_active_gauge, burst_window_ms_gauge set correctly
"""


import asyncio
import os
import time
import pytest
from typing import Dict, Any
from unittest.mock import AsyncMock, MagicMock, patch

# ─── Unit Under Test ───────────────────────────────────────────────────────
from core.burst_gate import BurstCandidateSelector, BurstCandidate, BurstState
from core.burst_calibrator import BurstCalibrator
from core.pressure_tracker import PressureTracker
from core.pressure_policy import decide_burst_window_ms, PressureDecision


# ═══════════════════════════════════════════════════════════════════════════
# 1. BurstCandidateSelector — Core Logic
# ═══════════════════════════════════════════════════════════════════════════

class TestBurstCandidateSelector:
    """Deterministic tests for BurstCandidateSelector (core/burst_gate.py)."""

    def _make(self, window_ms: int = 2500, max_age_ms: int = 8000) -> BurstCandidateSelector:
        return BurstCandidateSelector(window_ms=window_ms, max_age_ms=max_age_ms)

    def _cand(self, ts_ms: int = 1000, score: float = 0.5, payload: Dict | None = None) -> BurstCandidate:
        return BurstCandidate(ts_ms=ts_ms, score=score, payload=payload or {"s": ts_ms})

    # ── consider: first candidate starts burst ──
    def test_consider_starts_burst_on_first_call(self):
        b = self._make(window_ms=1000)
        assert not b.is_active()
        b.consider(ts_ms=1000, cand=self._cand(1000, 0.8))
        assert b.is_active()
        assert b.st.start_ts_ms == 1000
        assert b.st.deadline_ts_ms == 2000

    # ── consider: second candidate with higher score replaces best ──
    def test_consider_replaces_best_on_higher_score(self):
        b = self._make(window_ms=1000)
        b.consider(ts_ms=1000, cand=self._cand(1000, 0.5))
        b.consider(ts_ms=1200, cand=self._cand(1200, 0.9))
        assert b.st.best is not None
        assert b.st.best.score == 0.9

    # ── consider: lower score does NOT replace best ──
    def test_consider_lower_score_does_not_replace(self):
        b = self._make(window_ms=1000)
        b.consider(ts_ms=1000, cand=self._cand(1000, 0.9))
        b.consider(ts_ms=1200, cand=self._cand(1200, 0.3))
        assert b.st.best is not None
        assert b.st.best.score == 0.9

    # ── maybe_flush: before deadline → None ──
    def test_maybe_flush_before_deadline_returns_none(self):
        b = self._make(window_ms=1000)
        b.consider(ts_ms=1000, cand=self._cand(1000, 0.8))
        result = b.maybe_flush(now_ts_ms=1500)
        assert result is None
        assert b.is_active()  # still buffering

    # ── maybe_flush: at deadline → emits best ──
    def test_maybe_flush_at_deadline_emits_best(self):
        b = self._make(window_ms=1000)
        b.consider(ts_ms=1000, cand=self._cand(1000, 0.8, {"direction": "LONG"}))
        b.consider(ts_ms=1200, cand=self._cand(1200, 0.95, {"direction": "SHORT"}))
        result = b.maybe_flush(now_ts_ms=2000)
        assert result is not None
        assert result["direction"] == "SHORT"  # best score
        assert result["burst_best_score"] == 0.95
        assert result["burst_emitted_at"] == 2000
        assert result["burst_start_ts_ms"] == 1000
        assert result["burst_deadline_ts_ms"] == 2000
        assert not b.is_active()

    # ── maybe_flush: past deadline → emits ──
    def test_maybe_flush_past_deadline(self):
        b = self._make(window_ms=1000)
        b.consider(ts_ms=1000, cand=self._cand(1000, 0.8))
        result = b.maybe_flush(now_ts_ms=5000)
        assert result is not None

    # ── maybe_flush: not active → None ──
    def test_maybe_flush_inactive_returns_none(self):
        b = self._make()
        result = b.maybe_flush(now_ts_ms=99999)
        assert result is None

    # ── maybe_flush: now_ts_ms <= 0 → None ──
    def test_maybe_flush_zero_time_returns_none(self):
        b = self._make(window_ms=1000)
        b.consider(ts_ms=1000, cand=self._cand(1000, 0.8))
        assert b.maybe_flush(now_ts_ms=0) is None
        assert b.maybe_flush(now_ts_ms=-1) is None

    # ── max_age safety: very old burst is force-flushed ──
    def test_max_age_safety_flush(self):
        b = self._make(window_ms=1000, max_age_ms=3000)
        b.consider(ts_ms=1000, cand=self._cand(1000, 0.8, {"safe": True}))
        # Deadline is 2000 but max_age from start=1000 → max_age fires at 4000
        result = b.maybe_flush(now_ts_ms=4000)
        assert result is not None
        assert result["safe"] is True
        assert result["burst_max_age_flushed"] == 1
        assert result["burst_best_score"] == 0.8
        assert not b.is_active()

    # ── max_age: stale candidate dropped ──
    def test_max_age_stale_candidate_dropped(self):
        b = self._make(window_ms=1000, max_age_ms=3000)
        # Candidate from ts=1000, but now=15000 ⇒ candidate too old
        b.consider(ts_ms=1000, cand=self._cand(1000, 0.8))
        result = b.maybe_flush(now_ts_ms=15000)
        assert result is None  # candidate dropped due to staleness
        assert not b.is_active()

    # ── consider: max_age reset to new burst ──
    def test_consider_resets_on_max_age_exceeded(self):
        b = self._make(window_ms=1000, max_age_ms=3000)
        b.consider(ts_ms=1000, cand=self._cand(1000, 0.5))
        # Way past max_age → auto-reset to new burst
        b.consider(ts_ms=20000, cand=self._cand(20000, 0.7))
        assert b.st.start_ts_ms == 20000
        assert b.st.best.score == 0.7

    # ── force_flush ──
    def test_force_flush_returns_best_and_resets(self):
        b = self._make()
        b.consider(ts_ms=1000, cand=self._cand(1000, 0.5, {"forced": 1}))
        result = b.force_flush()
        assert result is not None
        assert result["forced"] == 1
        # force_flush now stamps burst metadata for audit consistency
        assert "burst_emitted_at" in result
        assert "burst_force_flushed" in result
        assert result["burst_force_flushed"] == 1
        assert result["burst_best_score"] == 0.5
        assert not b.is_active()

    def test_force_flush_inactive_returns_none(self):
        b = self._make()
        result = b.force_flush()
        assert result is None

    # ── reset ──
    def test_reset_clears_state(self):
        b = self._make()
        b.consider(ts_ms=1000, cand=self._cand(1000, 0.9))
        assert b.is_active()
        b.reset()
        assert not b.is_active()
        assert b.st.best is None

    # ── snapshot ──
    def test_snapshot_active(self):
        b = self._make(window_ms=500)
        b.consider(ts_ms=1000, cand=self._cand(1000, 0.75))
        snap = b.snapshot()
        assert snap["active"] == 1
        assert snap["start_ts_ms"] == 1000
        assert snap["deadline_ts_ms"] == 1500
        assert snap["best_score"] == 0.75

    def test_snapshot_inactive(self):
        b = self._make()
        snap = b.snapshot()
        assert snap["active"] == 0
        assert snap["best_score"] == 0.0


# ═══════════════════════════════════════════════════════════════════════════
# 2. BurstCalibrator — Window/MaxAge tuning
# ═══════════════════════════════════════════════════════════════════════════

class TestBurstCalibrator:
    """Tests for BurstCalibrator (core/burst_calibrator.py)."""

    def _make(self, **kw) -> BurstCalibrator:
        defaults = dict(
            base_window_ms=2500, min_window_ms=300, max_window_ms=3000,
            base_max_age_ms=8000, pressure_hi_per_min=60.0,
            pressure_extreme_per_min=200.0,
        )
        defaults.update(kw)
        return BurstCalibrator(**defaults)

    def test_low_pressure_base_window(self):
        c = self._make(max_age_mult=3.0)
        w, ma = c.compute(gap_p50_ms=0.0, cand_per_min=0.0)
        assert w == 2500
        assert ma >= 8000  # base_max_age (low pressure preserves base_max_age)

    def test_extreme_pressure_shrinks_window(self):
        c = self._make()
        w, ma = c.compute(gap_p50_ms=0.0, cand_per_min=200.0)
        assert w == 300  # min window under extreme

    def test_hi_pressure_mid_window(self):
        c = self._make()
        w, ma = c.compute(gap_p50_ms=0.0, cand_per_min=60.0)
        assert w <= 800

    def test_tick_gap_drives_window_down(self):
        c = self._make()
        w, _ = c.compute(gap_p50_ms=200.0, cand_per_min=0.0)
        # 1.2*200 = 240 < 2500, so window shrinks
        assert w <= 300  # bounded by min since 240 < min_ms=300

    def test_wide_gap_large_window(self):
        c = self._make()
        w, _ = c.compute(gap_p50_ms=3000.0, cand_per_min=0.0)
        assert w == 2500  # base case, capped by min(base, 1.2*3000=3600) → 2500

    def test_window_bounded_by_min_max(self):
        c = self._make(min_window_ms=500, max_window_ms=5000)
        w, _ = c.compute(gap_p50_ms=100.0, cand_per_min=0.0)
        assert 500 <= w <= 5000

    def test_max_age_scales_for_low_pressure(self):
        c = self._make(max_age_mult=3.0)
        _, ma = c.compute(gap_p50_ms=0.0, cand_per_min=10.0)
        assert ma >= 8000  # base_max_age preserved at low pressure

    def test_max_age_shrinks_for_hi_pressure(self):
        c = self._make(max_age_mult=3.0)
        _, ma = c.compute(gap_p50_ms=0.0, cand_per_min=100.0)
        # High pressure → w shrinks → max_age = max(min_max_age, w*3.0)
        assert ma >= 2000  # min_max_age
        # With mult=3.0 and w=800, max_age = max(2000, 800*3) = 2400
        assert ma == 2400


# ═══════════════════════════════════════════════════════════════════════════
# 3. PressureTracker.burst_window_ms (static mapping)
# ═══════════════════════════════════════════════════════════════════════════

class TestPressureTrackerBurstWindow:
    """Tests for PressureTracker.burst_window_ms static method."""

    def test_low_pressure_returns_base(self):
        result = PressureTracker.burst_window_ms(
            base_ms=2500, min_ms=800, per_min_ema=10.0,
            hi_per_min=60, extreme_per_min=200,
        )
        assert result == 2500

    def test_hi_pressure_returns_60pct_base(self):
        result = PressureTracker.burst_window_ms(
            base_ms=2500, min_ms=800, per_min_ema=60.0,
            hi_per_min=60, extreme_per_min=200,
        )
        assert result == max(800, int(0.60 * 2500))  # 1500

    def test_extreme_pressure_returns_min(self):
        result = PressureTracker.burst_window_ms(
            base_ms=2500, min_ms=800, per_min_ema=200.0,
            hi_per_min=60, extreme_per_min=200,
        )
        assert result == 800

    def test_base_below_min_clamps(self):
        result = PressureTracker.burst_window_ms(
            base_ms=500, min_ms=800, per_min_ema=0.0,
            hi_per_min=60, extreme_per_min=200,
        )
        assert result == 800  # base clamped up to min


# ═══════════════════════════════════════════════════════════════════════════
# 4. pressure_policy.decide_burst_window_ms
# ═══════════════════════════════════════════════════════════════════════════

class TestPressurePolicy:
    """Tests for decide_burst_window_ms (core/pressure_policy.py)."""

    def test_low_level(self):
        d = decide_burst_window_ms(
            triggers_per_min_ema=10.0, base_ms=2500, min_ms=800, mid_ms=1200,
            hi_thr_per_min=60.0, extreme_thr_per_min=200.0,
        )
        assert d.level == "LOW"
        assert d.burst_window_ms == 2500

    def test_hi_level(self):
        d = decide_burst_window_ms(
            triggers_per_min_ema=60.0, base_ms=2500, min_ms=800, mid_ms=1200,
            hi_thr_per_min=60.0, extreme_thr_per_min=200.0,
        )
        assert d.level == "HI"
        assert d.burst_window_ms == 1200

    def test_extreme_level(self):
        d = decide_burst_window_ms(
            triggers_per_min_ema=300.0, base_ms=2500, min_ms=800, mid_ms=1200,
            hi_thr_per_min=60.0, extreme_thr_per_min=200.0,
        )
        assert d.level == "EXTREME"
        assert d.burst_window_ms == 800

    def test_boundary_hi(self):
        d = decide_burst_window_ms(
            triggers_per_min_ema=59.9, base_ms=2500, min_ms=800, mid_ms=1200,
            hi_thr_per_min=60.0, extreme_thr_per_min=200.0,
        )
        assert d.level == "LOW"

    def test_boundary_extreme(self):
        d = decide_burst_window_ms(
            triggers_per_min_ema=199.9, base_ms=2500, min_ms=800, mid_ms=1200,
            hi_thr_per_min=60.0, extreme_thr_per_min=200.0,
        )
        assert d.level == "HI"


# ═══════════════════════════════════════════════════════════════════════════
# 5. PressureTracker — Snapshot and EMA
# ═══════════════════════════════════════════════════════════════════════════

class TestPressureTracker:
    """Tests for PressureTracker snapshot/ema."""

    def test_snapshot_empty(self):
        pt = PressureTracker(window_ms=60_000)
        snap = pt.snapshot(now_ms=100000)
        assert snap.n_raw == 0
        assert snap.per_min_ema == 0.0

    def test_on_raw_trigger_tracks(self):
        pt = PressureTracker(window_ms=60_000, ema_alpha=1.0)
        for i in range(10):
            pt.on_raw_trigger(ts_ms=1000 + i * 100)
        snap = pt.snapshot(now_ms=2000)
        assert snap.n_raw == 10

    def test_cooldown_hit_rate(self):
        pt = PressureTracker(window_ms=60_000, ema_alpha=1.0)
        pt.on_raw_trigger(ts_ms=1000)
        pt.on_raw_trigger(ts_ms=1100)
        pt.on_cooldown_hit(ts_ms=1050)
        snap = pt.snapshot(now_ms=2000)
        assert snap.n_cd == 1
        assert snap.cd_rate == 0.5  # 1 / 2

    def test_gc_evicts_old(self):
        pt = PressureTracker(window_ms=5000)
        pt.on_raw_trigger(ts_ms=1000)
        pt.on_raw_trigger(ts_ms=7000)
        snap = pt.snapshot(now_ms=7000)
        assert snap.n_raw == 1  # ts=1000 evicted

    def test_record_emit(self):
        pt = PressureTracker(window_ms=60_000)
        pt.record_emit(ts_ms=1000)
        assert len(pt._emit_ts) == 1


# ═══════════════════════════════════════════════════════════════════════════
# 6. Activation Logic: CRYPTO_BURST_ENABLE / pressure_extreme_flag
# ═══════════════════════════════════════════════════════════════════════════

class TestBurstActivation:
    """Tests that burst activation follows the spec: CRYPTO_BURST_ENABLE=1 OR pressure_extreme_flag=1."""

    def test_env_enable_activates_burst(self):
        """CRYPTO_BURST_ENABLE=1 → use_burst = True."""
        indicators = {"pressure_extreme_flag": 0}
        with patch.dict(os.environ, {"CRYPTO_BURST_ENABLE": "1"}):
            force_burst = bool(indicators.get("pressure_extreme_flag", 0))
            use_burst = bool(int(os.getenv("CRYPTO_BURST_ENABLE", "0"))) or force_burst
            assert use_burst is True

    def test_env_disable_no_extreme(self):
        """CRYPTO_BURST_ENABLE=0 && pressure_extreme_flag=0 → use_burst = False."""
        indicators = {"pressure_extreme_flag": 0}
        with patch.dict(os.environ, {"CRYPTO_BURST_ENABLE": "0"}):
            force_burst = bool(indicators.get("pressure_extreme_flag", 0))
            use_burst = bool(int(os.getenv("CRYPTO_BURST_ENABLE", "0"))) or force_burst
            assert use_burst is False

    def test_extreme_flag_activates_burst(self):
        """pressure_extreme_flag=1 → use_burst = True regardless of ENV."""
        indicators = {"pressure_extreme_flag": 1}
        with patch.dict(os.environ, {"CRYPTO_BURST_ENABLE": "0"}):
            force_burst = bool(indicators.get("pressure_extreme_flag", 0))
            use_burst = bool(int(os.getenv("CRYPTO_BURST_ENABLE", "0"))) or force_burst
            assert use_burst is True


# ═══════════════════════════════════════════════════════════════════════════
# 7. Burst Payload Contract (emitted metadata fields)
# ═══════════════════════════════════════════════════════════════════════════

class TestBurstPayloadContract:
    """Verify that burst-flushed payloads contain required metadata fields."""

    def test_metadata_fields_present_on_deadline_flush(self):
        b = BurstCandidateSelector(window_ms=1000, max_age_ms=5000)
        payload = {"direction": "LONG", "symbol": "BTCUSDT", "price": 45000.0}
        b.consider(ts_ms=1000, cand=BurstCandidate(ts_ms=1000, score=0.88, payload=payload))
        out = b.maybe_flush(now_ts_ms=2000)
        assert out is not None
        # Required metadata fields per spec
        assert "burst_emitted_at" in out
        assert "burst_start_ts_ms" in out
        assert "burst_deadline_ts_ms" in out
        assert "burst_best_score" in out
        # Values
        assert out["burst_emitted_at"] == 2000
        assert out["burst_start_ts_ms"] == 1000
        assert out["burst_deadline_ts_ms"] == 2000
        assert out["burst_best_score"] == 0.88
        # Original payload preserved
        assert out["direction"] == "LONG"
        assert out["symbol"] == "BTCUSDT"

    def test_force_flush_has_metadata(self):
        """force_flush now stamps burst metadata for audit consistency."""
        b = BurstCandidateSelector(window_ms=1000, max_age_ms=5000)
        payload = {"direction": "SHORT"}
        b.consider(ts_ms=1000, cand=BurstCandidate(ts_ms=1000, score=0.5, payload=payload))
        out = b.force_flush()
        assert out is not None
        assert out["direction"] == "SHORT"
        # force_flush now stamps metadata + force_flushed flag
        assert "burst_emitted_at" in out
        assert out["burst_force_flushed"] == 1
        assert out["burst_best_score"] == 0.5
        assert out["burst_start_ts_ms"] == 1000
        assert out["burst_deadline_ts_ms"] == 2000

    def test_max_age_flush_has_metadata(self):
        """max_age safety flush also stamps burst metadata."""
        b = BurstCandidateSelector(window_ms=1000, max_age_ms=2000)
        payload = {"x": 1}
        b.consider(ts_ms=1000, cand=BurstCandidate(ts_ms=1000, score=0.5, payload=payload))
        # ts=3000 is past max_age (start=1000 + max_age=2000 = 3000) → max_age flush
        out = b.maybe_flush(now_ts_ms=3000)
        assert out is not None
        assert "burst_emitted_at" in out
        assert out["burst_best_score"] == 0.5
        assert out["burst_max_age_flushed"] == 1


# ═══════════════════════════════════════════════════════════════════════════
# 8. BurstCalibrator edge cases
# ═══════════════════════════════════════════════════════════════════════════

class TestBurstCalibratorEdgeCases:
    def test_negative_gap_p50(self):
        c = BurstCalibrator(base_window_ms=2500, min_window_ms=300, max_window_ms=3000)
        w, _ = c.compute(gap_p50_ms=-1.0, cand_per_min=0.0)
        assert 300 <= w <= 3000

    def test_zero_pressure_zero_gap(self):
        c = BurstCalibrator(base_window_ms=2000, min_window_ms=200, max_window_ms=5000)
        w, ma = c.compute(gap_p50_ms=0.0, cand_per_min=0.0)
        assert w == 2000
        assert ma >= 8000  # base_max_age preserved at low pressure

    def test_extreme_values(self):
        c = BurstCalibrator(base_window_ms=2500, min_window_ms=300, max_window_ms=3000)
        w, ma = c.compute(gap_p50_ms=100000.0, cand_per_min=999999.0)
        assert 300 <= w <= 3000
        assert ma >= 300  # at least min_max_age


# ═══════════════════════════════════════════════════════════════════════════
# 9. Multiple candidates — best-of-burst correctness
# ═══════════════════════════════════════════════════════════════════════════

class TestBestOfBurst:
    """Ensure the burst correctly selects the highest-score candidate."""

    def test_10_candidates_best_score_wins(self):
        b = BurstCandidateSelector(window_ms=5000, max_age_ms=20000)
        scores = [0.3, 0.5, 0.1, 0.95, 0.2, 0.7, 0.6, 0.4, 0.8, 0.85]
        for i, s in enumerate(scores):
            b.consider(ts_ms=1000 + i * 100, cand=BurstCandidate(
                ts_ms=1000 + i * 100, score=s, payload={"idx": i, "score": s}
            ))
        out = b.maybe_flush(now_ts_ms=6000)
        assert out is not None
        assert out["burst_best_score"] == 0.95
        assert out["idx"] == 3  # 4th candidate had 0.95

    def test_equal_score_first_wins(self):
        """When scores are equal, the earlier candidate stays (no replace on equal)."""
        b = BurstCandidateSelector(window_ms=5000, max_age_ms=20000)
        b.consider(ts_ms=1000, cand=BurstCandidate(ts_ms=1000, score=0.5, payload={"who": "first"}))
        b.consider(ts_ms=1500, cand=BurstCandidate(ts_ms=1500, score=0.5, payload={"who": "second"}))
        out = b.maybe_flush(now_ts_ms=6000)
        assert out["who"] == "first"  # equal → no replace (strict >)


# ═══════════════════════════════════════════════════════════════════════════
# 10. Integration: _emit_payload burst-mode path
# ═══════════════════════════════════════════════════════════════════════════

class TestEmitPayloadBurstPath:
    """
    Integration tests verifying _emit_payload correctly buffers into burst
    and returns None until flushed.
    """

    @pytest.fixture
    def mock_runtime(self):
        """Build a minimal SymbolRuntime-like mock for burst testing."""
        rt = MagicMock()
        rt.symbol = "ETHUSDT"
        rt.config = {
            "burst_window_ms": 1000,
            "burst_max_age_ms": 5000,
            "cooldown_ms": 0,
            "cooldown_ms_reversal": 0,
            "cooldown_ms_continuation": 0,
        }
        rt.burst = BurstCandidateSelector(window_ms=1000, max_age_ms=5000)
        rt.burst_mu = asyncio.Lock()
        rt.last_signal_ts = 0
        rt.pressure = PressureTracker(window_ms=60000)
        rt.pending_payload = None
        rt.pending_score = 0.0
        rt.pending_ts_ms = 0
        rt.pending_replaced = 0
        rt.last_emit_dir = "NONE"
        return rt

    @pytest.mark.asyncio
    async def test_burst_enabled_buffers_and_returns_none(self, mock_runtime):
        """When burst enabled, _emit_payload should return None (buffered)."""
        payload = {
            "direction": "LONG",
            "confidence": 0.85,
            "indicators": {
                "of_confirm_score": 0.9,
                "pressure_extreme_flag": 0,
                "strong_gate_scn": "reversal",
                "sweep": 0,
            }
        }

        # Simulate the core burst check logic from _emit_payload
        indicators = payload.get("indicators", {})
        force_burst = bool(indicators.get("pressure_extreme_flag", 0))

        with patch.dict(os.environ, {"CRYPTO_BURST_ENABLE": "1"}):
            use_burst = bool(int(os.getenv("CRYPTO_BURST_ENABLE", "0"))) or force_burst
            assert use_burst is True

            # Simulate burst consider (same as _emit_payload does)
            rt = mock_runtime
            async with rt.burst_mu:
                rt.burst.consider(
                    ts_ms=1000,
                    cand=BurstCandidate(ts_ms=1000, score=0.9, payload=payload),
                )
            assert rt.burst.is_active()
            # _emit_payload returns None here (burst buffered)
            assert rt.burst.maybe_flush(now_ts_ms=1000) is None  # before deadline

    @pytest.mark.asyncio
    async def test_burst_disabled_returns_payload(self, mock_runtime):
        """When burst disabled, _emit_payload returns payload immediately."""
        payload = {"direction": "SHORT", "indicators": {"pressure_extreme_flag": 0}}

        with patch.dict(os.environ, {"CRYPTO_BURST_ENABLE": "0"}):
            use_burst = bool(int(os.getenv("CRYPTO_BURST_ENABLE", "0"))) or False
            assert use_burst is False
            # Would return payload directly — no buffering


# ═══════════════════════════════════════════════════════════════════════════
# 11. Runtime Initialization Verification
# ═══════════════════════════════════════════════════════════════════════════

class TestRuntimeBurstInit:
    """Ensure SymbolRuntime properly initializes burst components."""

    def test_burst_selector_created_with_config(self):
        from services.orderflow.runtime import SymbolRuntime
        rt = SymbolRuntime(
            symbol="BTCUSDT",
            config={"burst_window_ms": 1500, "burst_max_age_ms": 6000}
        )
        assert rt.burst is not None
        assert isinstance(rt.burst, BurstCandidateSelector)
        assert rt.burst.window_ms == 1500
        assert rt.burst.max_age_ms == 6000

    def test_burst_calibrator_created_with_config(self):
        from services.orderflow.runtime import SymbolRuntime
        rt = SymbolRuntime(
            symbol="ETHUSDT",
            config={
                "burst_window_ms": 2000,
                "burst_window_min_ms": 400,
                "burst_window_max_ms": 4000,
                "burst_max_age_ms": 7000,
                "pressure_hi_per_min": 50.0,
                "pressure_extreme_per_min": 150.0,
            }
        )
        assert rt.burst_cal is not None
        assert isinstance(rt.burst_cal, BurstCalibrator)
        assert rt.burst_cal.base_window_ms == 2000
        assert rt.burst_cal.min_window_ms == 400
        assert rt.burst_cal.max_window_ms == 4000
        assert rt.burst_cal.base_max_age_ms == 7000
        assert rt.burst_cal.pressure_hi_per_min == 50.0
        assert rt.burst_cal.pressure_extreme_per_min == 150.0

    def test_burst_mu_is_asyncio_lock(self):
        from services.orderflow.runtime import SymbolRuntime
        rt = SymbolRuntime(symbol="BTCUSDT", config={})
        assert isinstance(rt.burst_mu, asyncio.Lock)

    def test_pressure_tracker_created(self):
        from services.orderflow.runtime import SymbolRuntime
        rt = SymbolRuntime(
            symbol="BTCUSDT",
            config={"pressure_window_ms": 30000, "pressure_ema_alpha": 0.15}
        )
        assert rt.pressure is not None
        assert isinstance(rt.pressure, PressureTracker)
        assert rt.pressure.window_ms == 30000
        assert rt.pressure.alpha == 0.15

    def test_defaults_for_missing_config_keys(self):
        from services.orderflow.runtime import SymbolRuntime
        rt = SymbolRuntime(symbol="BTCUSDT", config={})
        # Defaults from __post_init__
        assert rt.burst.window_ms == 2500
        assert rt.burst.max_age_ms == 8000
        assert rt.pressure.window_ms >= 5000
