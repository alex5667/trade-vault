from __future__ import annotations

"""Tests for BookProcessor book missing-seq continuity EMA tracking (v1).

Tests validate:
- _ema_update: safe alpha clamping, correct EMA formula
- _update_book_missing_seq: gap detection, EMA accumulation,
  reorder/duplicate handling, partial depth (no U/u), monotone last_u
"""


import pytest
import contextlib

# ---------------------------------------------------------------------------
# Import guard
# ---------------------------------------------------------------------------
try:
    from services.orderflow.components.book_processor import BookProcessor
except Exception as exc:
    pytest.skip(f"could not import BookProcessor: {exc}", allow_module_level=True)

with contextlib.suppress(Exception):
    from types import SimpleNamespace


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_runtime(alpha: float = 0.1) -> SimpleNamespace:
    """Create a minimal SymbolRuntime-like namespace for book_processor tests."""
    return SimpleNamespace(
        symbol="BTCUSDT",
        config={"book_missing_seq_ema_alpha": alpha},
        book_seq_last_u=0,
        book_seq_last_reason="init",
        book_missing_seq_ema=0.0,
    )


def _make_book_raw(U: int = 0, u: int = 0) -> dict:
    return {"U": U, "u": u, "bids": [], "asks": []}


# ---------------------------------------------------------------------------
# _ema_update tests
# ---------------------------------------------------------------------------

class TestEmaUpdate:
    def test_basic_ema(self):
        result = BookProcessor._ema_update(0.0, 1.0, 0.5)
        assert result == pytest.approx(0.5)

    def test_zero_prev(self):
        result = BookProcessor._ema_update(0.0, 0.0, 0.1)
        assert result == pytest.approx(0.0)

    def test_alpha_clamped_below(self):
        # alpha=0 is invalid, should be reset to 0.1
        result = BookProcessor._ema_update(0.0, 1.0, 0.0)
        assert result == pytest.approx(0.1)  # default alpha 0.1 applied

    def test_alpha_clamped_above(self):
        # alpha > 1 is invalid, should be reset to 0.1
        result = BookProcessor._ema_update(0.0, 1.0, 1.5)
        assert result == pytest.approx(0.1)

    def test_alpha_one(self):
        # alpha=1.0 valid: new_ema = 1.0 * x + 0 * prev = x
        result = BookProcessor._ema_update(0.5, 1.0, 1.0)
        assert result == pytest.approx(1.0)

    def test_invalid_prev_type(self):
        result = BookProcessor._ema_update("bad", 1.0, 0.5)
        assert result == pytest.approx(0.5)

    def test_invalid_x_type(self):
        result = BookProcessor._ema_update(0.5, "bad", 0.5)
        assert result == pytest.approx(0.25)  # 0.5*0 + 0.5*0.5


# ---------------------------------------------------------------------------
# _update_book_missing_seq tests
# ---------------------------------------------------------------------------

class TestUpdateBookMissingSeq:
    def setup_method(self):
        self.proc = BookProcessor()

    def test_first_event_no_continuity_check(self):
        """First event: prev_u=0, no check possible => reason=init, no EMA change."""
        rt = _make_runtime()
        book = _make_book_raw(U=100, u=150)
        self.proc._update_book_missing_seq(rt, book)
        assert rt.book_seq_last_reason == "init"
        assert rt.book_seq_last_u == 150  # advanced
        assert rt.book_missing_seq_ema == pytest.approx(0.0)

    def test_continuous_sequence(self):
        """Consecutive events with no gap: reason=ok, EMA stays near 0."""
        rt = _make_runtime(alpha=0.1)
        # init: prev_u=0->150
        self.proc._update_book_missing_seq(rt, _make_book_raw(U=100, u=150))
        # next: U=151 (== prev_u+1), u=200
        self.proc._update_book_missing_seq(rt, _make_book_raw(U=151, u=200))
        assert rt.book_seq_last_reason == "ok"
        assert rt.book_seq_last_u == 200
        # EMA: first update was init (0), second step: ok -> 0.1*0 + 0.9*0 = 0
        assert rt.book_missing_seq_ema == pytest.approx(0.0)

    def test_gap_increments_ema(self):
        """Gap (U > prev_u + 1): reason=gap, EMA > 0."""
        rt = _make_runtime(alpha=0.5)
        self.proc._update_book_missing_seq(rt, _make_book_raw(U=100, u=150))  # init
        # U=200 > 151 => gap
        self.proc._update_book_missing_seq(rt, _make_book_raw(U=200, u=250))
        assert rt.book_seq_last_reason == "gap"
        # EMA: prev=0.0, miss=1.0, alpha=0.5 → 0.5*1 + 0.5*0 = 0.5
        assert rt.book_missing_seq_ema == pytest.approx(0.5)

    def test_reorder_no_ema_change(self):
        """Duplicate/reorder (U < expected): reason=reorder_or_reset, EMA smoothed towards 0."""
        rt = _make_runtime(alpha=0.5)
        self.proc._update_book_missing_seq(rt, _make_book_raw(U=100, u=150))  # init, prev_u=150
        # U=50 < 151 => reorder
        self.proc._update_book_missing_seq(rt, _make_book_raw(U=50, u=55))
        assert rt.book_seq_last_reason == "reorder_or_reset"
        # miss_event=0 => EMA = 0.5*0 + 0.5*0 = 0
        assert rt.book_missing_seq_ema == pytest.approx(0.0)
        # prev_u must NOT decrease (monotone guard)
        assert rt.book_seq_last_u == 150

    def test_no_u_field_skips_continuity(self):
        """Events without u field: reason=no_u, EMA decays if running."""
        rt = _make_runtime(alpha=0.1)
        rt.book_missing_seq_ema = 0.3  # set arbitrary
        self.proc._update_book_missing_seq(rt, {"bids": [], "asks": []})  # no U, no u
        assert rt.book_seq_last_reason == "no_u"
        assert rt.book_missing_seq_ema == pytest.approx(0.27)  # 0.9 * 0.3

    def test_partial_depth_no_U_field(self):
        """Partial depth snapshots (@depth5): u present but U=0 => reason=no_seq_fields."""
        rt = _make_runtime()
        rt.book_seq_last_u = 0
        # u=999 but U=0 (partial depth: only lastUpdateId, no firstUpdateId)
        self.proc._update_book_missing_seq(rt, _make_book_raw(U=0, u=999))
        # prev_u was 0 too, so no check possible
        assert rt.book_seq_last_reason in ("init", "no_seq_fields")

    def test_u_monotone_advance(self):
        """last_u advances only when u increases (monotone)."""
        rt = _make_runtime()
        self.proc._update_book_missing_seq(rt, _make_book_raw(U=10, u=50))
        assert rt.book_seq_last_u == 50
        # Same u (duplicate): u=50 <= 50, no advance
        self.proc._update_book_missing_seq(rt, _make_book_raw(U=50, u=50))
        assert rt.book_seq_last_u == 50

    def test_consecutive_gaps_ema_accumulates(self):
        """Multiple consecutive gaps: EMA climbs towards 1."""
        rt = _make_runtime(alpha=0.5)
        self.proc._update_book_missing_seq(rt, _make_book_raw(U=100, u=199))  # init
        for i in range(5):
            u_prev = rt.book_seq_last_u
            # Big jump each time
            self.proc._update_book_missing_seq(rt, _make_book_raw(U=u_prev + 100, u=u_prev + 200))
        assert rt.book_missing_seq_ema > 0.9

    def test_recovery_ema_decreases(self):
        """After gaps, continuous events drive EMA back towards 0."""
        rt = _make_runtime(alpha=0.5)
        # Prime EMA with gaps
        self.proc._update_book_missing_seq(rt, _make_book_raw(U=100, u=199))
        self.proc._update_book_missing_seq(rt, _make_book_raw(U=500, u=600))  # gap → EMA = 0.5
        ema_after_gap = rt.book_missing_seq_ema

        # Now send 5 continuous events
        for _ in range(5):
            u_last = rt.book_seq_last_u
            self.proc._update_book_missing_seq(rt, _make_book_raw(U=u_last + 1, u=u_last + 10))

        assert rt.book_missing_seq_ema < ema_after_gap
