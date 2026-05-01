from __future__ import annotations
"""
Unit tests for Phase E / P4: MessageRateTracker

test_message_rate_tracker_p4_v1.py
"""

import pytest
from unittest.mock import MagicMock, patch


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_tracker(**kwargs):
    from services.orderflow.message_rate import MessageRateTracker
    return MessageRateTracker(**kwargs)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Basic construction
# ─────────────────────────────────────────────────────────────────────────────

class TestMessageRateTrackerConstruction:
    def test_defaults(self):
        t = _make_tracker()
        assert t.book_update_rate_hz == 0.0
        assert t.trade_msg_rate_hz == 0.0
        assert t.cancel_rate_z == 0.0
        assert t.otr == 0.0
        assert t.otr_z == 0.0

    def test_custom_alpha(self):
        t = _make_tracker(alpha=0.5, z_window=64)
        assert t.alpha == 0.5


# ─────────────────────────────────────────────────────────────────────────────
# 2. on_book_msg: bucket boundary logic
# ─────────────────────────────────────────────────────────────────────────────

class TestOnBookMsg:
    def test_first_message_no_update(self):
        """First message should not update rate until bucket boundary crosses."""
        t = _make_tracker()
        t.on_book_msg(1_000_000)
        # Still in first bucket, no EMA update yet
        assert t.book_update_rate_hz == 0.0

    def test_bucket_boundary_updates_rate(self):
        """Messages count accumulates then EMA updates on bucket change."""
        t = _make_tracker(alpha=1.0)  # alpha=1.0 => EMA = latest inst
        # Send 5 messages in bucket 1000 (ts_ms 1000_000..1000_999)
        for i in range(5):
            t.on_book_msg(1_000_000 + i * 100)
        # Cross bucket boundary
        t.on_book_msg(1_001_000)
        # With alpha=1.0: EMA = 5 (count in previous bucket)
        assert t.book_update_rate_hz == 5.0

    def test_multiple_buckets(self):
        """Multiple bucket crossings accumulate EMA across buckets."""
        t = _make_tracker(alpha=1.0)
        # bucket 1000: 3 msgs
        for _ in range(3):
            t.on_book_msg(1_000_000)
        # cross to bucket 1001: flush 3 msgs, then add 7
        for _ in range(7):
            t.on_book_msg(1_001_000)
        # cross to bucket 1002: flush 7 msgs
        t.on_book_msg(1_002_000)
        assert t.book_update_rate_hz == 7.0  # last flushed count

    def test_silent_exception_on_bad_ts(self):
        """on_book_msg must never raise even on bad inputs."""
        t = _make_tracker()
        t.on_book_msg(-1)
        t.on_book_msg(0)
        t.on_book_msg(None)  # type: ignore
        # all fail-open
        assert t.book_update_rate_hz == 0.0

    def test_z_score_updates_after_warmup(self):
        """After enough buckets, z-score should become non-zero for unusual rate."""
        t = _make_tracker(alpha=0.5, z_window=8)
        base = 1_000_000
        # Warm up with stable rate (~10 msg/s per bucket)
        for bucket in range(20):
            for _ in range(10):
                t.on_book_msg(base + bucket * 1000)
        # Cross final boundary with spike
        for _ in range(50):
            t.on_book_msg(base + 21 * 1000)
        t.on_book_msg(base + 22 * 1000)
        # After spike, z-score should be elevated
        assert t.book_update_rate_z > 0.0


# ─────────────────────────────────────────────────────────────────────────────
# 3. on_trade_msg
# ─────────────────────────────────────────────────────────────────────────────

class TestOnTradeMsg:
    def test_trade_rate_updates_on_bucket_boundary(self):
        t = _make_tracker(alpha=1.0)
        for _ in range(4):
            t.on_trade_msg(2_000_000)
        t.on_trade_msg(2_001_000)
        assert t.trade_msg_rate_hz == 4.0

    def test_fail_open_none(self):
        t = _make_tracker()
        t.on_trade_msg(None)  # type: ignore
        assert t.trade_msg_rate_hz == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# 4. OTR
# ─────────────────────────────────────────────────────────────────────────────

class TestOTR:
    def test_otr_floor_on_zero_trade_rate(self):
        """OTR denominator is floored at 1.0; never division by zero."""
        t = _make_tracker(alpha=1.0)
        # Push book messages but no trades
        for _ in range(10):
            t.on_book_msg(1_000_000)
        t.on_book_msg(1_001_000)
        # trade_msg_rate_hz = 0 => denominator = 1 => OTR = book_rate / 1
        assert t.otr == t.book_update_rate_hz / 1.0

    def test_otr_normal(self):
        t = _make_tracker(alpha=1.0)
        # Book: 20 per bucket 1000
        for _ in range(20):
            t.on_book_msg(1_000_000)
        # Trade: 4 per bucket 1000
        for _ in range(4):
            t.on_trade_msg(1_000_000)
        # Cross boundary to flush bucket 1000 counts
        t.on_book_msg(1_001_000)
        t.on_trade_msg(1_001_000)
        # book_rate = 20, trade_rate = 4 => OTR = 20/4 = 5.0
        assert t.otr > 1.0


# ─────────────────────────────────────────────────────────────────────────────
# 5. observe_cancel_rate_ema
# ─────────────────────────────────────────────────────────────────────────────

class TestCancelRate:
    def test_cancel_rate_zero_initially(self):
        t = _make_tracker()
        assert t.cancel_rate_z == 0.0

    def test_cancel_rate_z_stabilizes_after_warmup(self):
        t = _make_tracker(z_window=8)
        # Feed stable cancel rate
        for _ in range(20):
            t.observe_cancel_rate_ema(0.1)
        # Spike
        t.observe_cancel_rate_ema(1.0)
        assert t.cancel_rate_z > 0.0

    def test_fail_open_none_cancel(self):
        t = _make_tracker()
        t.observe_cancel_rate_ema(None)  # type: ignore
        assert t.cancel_rate_z == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# 6. snapshot()
# ─────────────────────────────────────────────────────────────────────────────

class TestSnapshot:
    def test_snapshot_keys(self):
        t = _make_tracker()
        s = t.snapshot()
        assert set(s.keys()) == {
            "book_update_rate_hz",
            "book_update_rate_z",
            "trade_msg_rate_hz",
            "trade_msg_rate_z",
            "cancel_rate_z",
            "otr",
            "otr_z",
        }

    def test_snapshot_all_float(self):
        t = _make_tracker()
        s = t.snapshot()
        for k, v in s.items():
            assert isinstance(v, float), f"{k} must be float"
