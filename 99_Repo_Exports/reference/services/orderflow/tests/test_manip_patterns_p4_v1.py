"""
Unit tests for Phase E / P4: ManipulationTracker (quote stuffing + layering)

test_manip_patterns_p4_v1.py
"""
from __future__ import annotations

import pytest


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_tracker():
    from services.orderflow.manip_patterns import ManipulationTracker
    return ManipulationTracker()


def _book_call(tracker, *, ts_ms, bid_depth_usd=50_000.0, ask_depth_usd=50_000.0,
               book_update_rate_z=0.0, cancel_rate_z=0.0, trade_msg_rate_hz=5.0,
               mid_px=50_000.0):
    """Helper for calling update_from_book with defaults."""
    tracker.update_from_book(
        ts_ms=ts_ms,
        bid_depth_usd=bid_depth_usd,
        ask_depth_usd=ask_depth_usd,
        book_update_rate_z=book_update_rate_z,
        cancel_rate_z=cancel_rate_z,
        trade_msg_rate_hz=trade_msg_rate_hz,
        mid_px=mid_px,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 1. Basic construction
# ─────────────────────────────────────────────────────────────────────────────

class TestManipTrackerConstruction:
    def test_defaults(self):
        t = _make_tracker()
        assert t.quote_stuffing_score == 0.0
        assert t.layering_score == 0.0
        assert t.manip_flags == ""
        assert t._lay_state == "idle"

    def test_no_crash_on_zero_inputs(self):
        t = _make_tracker()
        _book_call(t, ts_ms=1_000_000)
        assert t.quote_stuffing_score == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# 2. Quote Stuffing detection
# ─────────────────────────────────────────────────────────────────────────────

class TestQuoteStuffing:
    def test_no_stuffing_below_threshold(self, monkeypatch):
        """When z-scores are below thresholds, no stuffing detected."""
        monkeypatch.setenv("QUOTE_STUFF_MSG_Z_THR", "4.0")
        monkeypatch.setenv("QUOTE_STUFF_CANCEL_Z_THR", "3.5")
        t = _make_tracker()
        # Feed low z-scores (below thresholds)
        _book_call(t, ts_ms=1_000_000, book_update_rate_z=1.0, cancel_rate_z=1.0)
        assert t.quote_stuffing_score == 0.0
        assert "QUOTE_STUFFING" not in t.manip_flags

    def test_stuffing_detected_when_both_above_threshold(self, monkeypatch):
        """Both book_update_rate_z AND cancel_rate_z above thresholds → stuffing."""
        monkeypatch.setenv("QUOTE_STUFF_MSG_Z_THR", "2.0")
        monkeypatch.setenv("QUOTE_STUFF_CANCEL_Z_THR", "1.5")
        t = _make_tracker()
        _book_call(t, ts_ms=1_000_000, book_update_rate_z=5.0, cancel_rate_z=4.0)
        assert t.quote_stuffing_score > 0.0
        assert "QUOTE_STUFFING" in t.manip_flags

    def test_no_stuffing_only_one_above_threshold(self, monkeypatch):
        """Only book_z above threshold BUT cancel_z below → no stuffing (AND logic)."""
        monkeypatch.setenv("QUOTE_STUFF_MSG_Z_THR", "2.0")
        monkeypatch.setenv("QUOTE_STUFF_CANCEL_Z_THR", "3.0")
        t = _make_tracker()
        # book_z high, cancel_z low
        _book_call(t, ts_ms=1_000_000, book_update_rate_z=6.0, cancel_rate_z=1.0)
        assert t.quote_stuffing_score == 0.0

    def test_stuffing_score_capped_at_1(self, monkeypatch):
        """quote_stuffing_score never exceeds 1.0."""
        monkeypatch.setenv("QUOTE_STUFF_MSG_Z_THR", "1.0")
        monkeypatch.setenv("QUOTE_STUFF_CANCEL_Z_THR", "1.0")
        t = _make_tracker()
        _book_call(t, ts_ms=1_000_000, book_update_rate_z=100.0, cancel_rate_z=100.0)
        assert t.quote_stuffing_score <= 1.0

    def test_disabled_when_thresholds_zero(self, monkeypatch):
        """Both thresholds = 0 → disabled (no detection)."""
        monkeypatch.setenv("QUOTE_STUFF_MSG_Z_THR", "0")
        monkeypatch.setenv("QUOTE_STUFF_CANCEL_Z_THR", "0")
        t = _make_tracker()
        _book_call(t, ts_ms=1_000_000, book_update_rate_z=100.0, cancel_rate_z=100.0)
        assert t.quote_stuffing_score == 0.0

    def test_score_decays_when_normal(self, monkeypatch):
        """After detection, score decays when z-scores return to normal."""
        monkeypatch.setenv("QUOTE_STUFF_MSG_Z_THR", "2.0")
        monkeypatch.setenv("QUOTE_STUFF_CANCEL_Z_THR", "1.5")
        t = _make_tracker()
        _book_call(t, ts_ms=1_000_000, book_update_rate_z=5.0, cancel_rate_z=4.0)
        score_peak = t.quote_stuffing_score
        assert score_peak > 0
        # Return to normal
        for i in range(10):
            _book_call(t, ts_ms=1_001_000 + i * 100, book_update_rate_z=0.5, cancel_rate_z=0.3)
        assert t.quote_stuffing_score < score_peak


# ─────────────────────────────────────────────────────────────────────────────
# 3. Layering detection
# ─────────────────────────────────────────────────────────────────────────────

class TestLayering:
    def test_no_layering_in_normal_market(self, monkeypatch):
        """No layering when trade rate is high or depth is stable."""
        monkeypatch.setenv("LAYERING_BUILD_MULT", "1.6")
        monkeypatch.setenv("LAYERING_TRADE_RATE_LOW_HZ", "2.0")
        monkeypatch.setenv("LAYERING_MIN_PEAK_USD", "5000")
        t = _make_tracker()
        # High trade rate → no layering
        for i in range(20):
            _book_call(t, ts_ms=1_000_000 + i * 100,
                       bid_depth_usd=50_000, ask_depth_usd=50_000,
                       trade_msg_rate_hz=10.0)
        assert t.layering_score == 0.0 or "LAYERING" not in t.manip_flags

    def test_layering_detected_build_then_revert(self, monkeypatch):
        """Build phase → quick revert within window → layering confirmed."""
        monkeypatch.setenv("LAYERING_BUILD_MULT", "1.5")
        monkeypatch.setenv("LAYERING_REVERT_FRAC", "0.35")
        monkeypatch.setenv("LAYERING_REVERT_MS", "900")
        monkeypatch.setenv("LAYERING_MIN_PEAK_USD", "1000")
        monkeypatch.setenv("LAYERING_TRADE_RATE_LOW_HZ", "3.0")
        monkeypatch.setenv("LAYERING_RATIO_MIN", "0.3")
        t = _make_tracker()

        # Warm up baseline (low trade rate = 1.0 Hz)
        for i in range(30):
            _book_call(t, ts_ms=1_000_000 + i * 1000,
                       bid_depth_usd=10_000, ask_depth_usd=10_000,
                       trade_msg_rate_hz=1.0)

        # Build phase: depth spikes on ask side (big enough to trigger)
        build_ts = 1_030_000
        _book_call(t, ts_ms=build_ts,
                   bid_depth_usd=10_000, ask_depth_usd=25_000,  # 2.5x spike
                   trade_msg_rate_hz=1.0)

        # Revert quickly (within 900ms)
        revert_ts = build_ts + 400
        _book_call(t, ts_ms=revert_ts,
                   bid_depth_usd=10_000, ask_depth_usd=8_000,  # snap back below peak
                   trade_msg_rate_hz=1.0)

        # Layering should have been detected
        assert t.layering_score > 0.0 or "LAYERING" in t.manip_flags

    def test_layering_not_detected_slow_revert(self, monkeypatch):
        """If depth doesn't revert within the window, no layering signal."""
        monkeypatch.setenv("LAYERING_BUILD_MULT", "1.5")
        monkeypatch.setenv("LAYERING_REVERT_FRAC", "0.35")
        monkeypatch.setenv("LAYERING_REVERT_MS", "900")
        monkeypatch.setenv("LAYERING_MIN_PEAK_USD", "1000")
        monkeypatch.setenv("LAYERING_TRADE_RATE_LOW_HZ", "3.0")
        t = _make_tracker()

        # Warm up
        for i in range(20):
            _book_call(t, ts_ms=1_000_000 + i * 1000,
                       bid_depth_usd=10_000, ask_depth_usd=10_000,
                       trade_msg_rate_hz=1.0)

        # Build phase
        build_ts = 1_020_000
        _book_call(t, ts_ms=build_ts,
                   bid_depth_usd=10_000, ask_depth_usd=25_000,
                   trade_msg_rate_hz=1.0)

        # Slow revert (> 900ms) → past window
        revert_ts = build_ts + 1500
        _book_call(t, ts_ms=revert_ts,
                   bid_depth_usd=10_000, ask_depth_usd=8_000,
                   trade_msg_rate_hz=1.0)

        # No layering because revert was too slow
        assert "LAYERING" not in t.manip_flags
        assert t.layering_score == 0.0 or t._lay_state == "idle"

    def test_layering_score_capped(self, monkeypatch):
        """layering_score must never exceed 1.0."""
        monkeypatch.setenv("LAYERING_BUILD_MULT", "1.1")
        monkeypatch.setenv("LAYERING_REVERT_FRAC", "0.1")
        monkeypatch.setenv("LAYERING_REVERT_MS", "10000")
        monkeypatch.setenv("LAYERING_MIN_PEAK_USD", "100")
        monkeypatch.setenv("LAYERING_TRADE_RATE_LOW_HZ", "100.0")  # always low trade rate
        monkeypatch.setenv("LAYERING_RATIO_MIN", "0.01")
        t = _make_tracker()

        # Trigger massive build
        for i in range(5):
            _book_call(t, ts_ms=1_000_000 + i * 100,
                       bid_depth_usd=100, ask_depth_usd=100, trade_msg_rate_hz=0.1)
        _book_call(t, ts_ms=1_001_000, bid_depth_usd=100, ask_depth_usd=100_000_000.0,
                   trade_msg_rate_hz=0.1)
        _book_call(t, ts_ms=1_001_500, bid_depth_usd=100, ask_depth_usd=100.0,
                   trade_msg_rate_hz=0.1)

        assert t.layering_score <= 1.0


# ─────────────────────────────────────────────────────────────────────────────
# 4. manip_flags string
# ─────────────────────────────────────────────────────────────────────────────

class TestManipFlags:
    def test_no_flags_in_normal_market(self, monkeypatch):
        monkeypatch.setenv("QUOTE_STUFF_MSG_Z_THR", "4.0")
        monkeypatch.setenv("QUOTE_STUFF_CANCEL_Z_THR", "3.5")
        t = _make_tracker()
        _book_call(t, ts_ms=1_000_000)
        assert t.manip_flags == ""

    def test_quote_stuffing_flag(self, monkeypatch):
        monkeypatch.setenv("QUOTE_STUFF_MSG_Z_THR", "1.0")
        monkeypatch.setenv("QUOTE_STUFF_CANCEL_Z_THR", "1.0")
        t = _make_tracker()
        _book_call(t, ts_ms=1_000_000, book_update_rate_z=5.0, cancel_rate_z=5.0)
        assert "QUOTE_STUFFING" in t.manip_flags


# ─────────────────────────────────────────────────────────────────────────────
# 5. Fail-open: bad/None inputs
# ─────────────────────────────────────────────────────────────────────────────

class TestFailOpen:
    def test_none_ts(self):
        t = _make_tracker()
        t.update_from_book(
            ts_ms=None,  # type: ignore
            bid_depth_usd=1000.0,
            ask_depth_usd=1000.0,
            book_update_rate_z=0.0,
            cancel_rate_z=0.0,
            trade_msg_rate_hz=5.0,
            mid_px=50_000.0,
        )
        # Should not raise
        assert t.quote_stuffing_score == 0.0

    def test_negative_depth(self):
        t = _make_tracker()
        t.update_from_book(
            ts_ms=1_000_000,
            bid_depth_usd=-100.0,
            ask_depth_usd=-200.0,
            book_update_rate_z=0.0,
            cancel_rate_z=0.0,
            trade_msg_rate_hz=5.0,
            mid_px=50_000.0,
        )
        assert t.layering_score == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# 6. snapshot()
# ─────────────────────────────────────────────────────────────────────────────

class TestManipSnapshot:
    def test_snapshot_keys(self):
        t = _make_tracker()
        s = t.snapshot()
        assert set(s.keys()) == {"quote_stuffing_score", "layering_score", "manip_flags"}

    def test_snapshot_types(self):
        t = _make_tracker()
        s = t.snapshot()
        assert isinstance(s["quote_stuffing_score"], float)
        assert isinstance(s["layering_score"], float)
        assert isinstance(s["manip_flags"], str)
