"""Tests for _update_book_seq_uu_dq() deterministic U/u sequence tracker.

Verifies:
- init step sets state correctly, no resync
- ok/overlap step advances state, no gap
- explicit gap step increases EMA, sets book_resync_requested when EMA >= threshold
"""
import os
import sys
from pathlib import Path

import pytest

# Walk up until we find a directory with both services/ and core/ (python-worker root).
_here = Path(__file__).resolve()
for _n in range(8):
    _cand = _here.parents[_n]
    if (_cand / "services").is_dir() and (_cand / "core").is_dir():
        if str(_cand) not in sys.path:
            sys.path.insert(0, str(_cand))
        break


# ---------------------------------------------------------------------------
# Minimal fake runtime (no Redis, no metrics, no EMA tracker)
# ---------------------------------------------------------------------------

class FakeRuntime:
    def __init__(self):
        self.symbol = "BTCUSDT"
        self.config = {}
        self.book_seq_last_u = 0
        self.book_ingest_last_ts_ms = 0
        self.book_seq_last_reason = ""
        self.book_missing_seq_last_gap = 0
        self.book_missing_seq_ema = 0.0
        self.book_resync_requested = False
        self.book_resync_reason = ""
        self.book_resync_action = "resubscribe"
        self.book_resync_last_ts_ms = 0
        self.book_seq_gap = None  # no EMA tracker; EMA stays at 0


try:
    from services.orderflow.components.book_processor import _update_book_seq_uu_dq
except Exception as exc:
    pytest.skip(f"could not import _update_book_seq_uu_dq: {exc}", allow_module_level=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_init_step_no_gap():
    """First message (prev_u==0) -> reason=init, no gap, book_seq_last_u=u."""
    rt = FakeRuntime()
    _update_book_seq_uu_dq(rt, ingest_ts_ms=1000, book_raw={"U": 100, "u": 150})
    assert rt.book_seq_last_reason == "init"
    assert rt.book_missing_seq_last_gap == 0
    assert rt.book_seq_last_u == 150
    assert rt.book_resync_requested is False


def test_ok_overlap_step():
    """Continuous sequence (cur_U <= prev_u+1 <= cur_u) -> reason=ok, no gap."""
    rt = FakeRuntime()
    rt.book_seq_last_u = 150
    _update_book_seq_uu_dq(rt, ingest_ts_ms=2000, book_raw={"U": 151, "u": 200})
    assert rt.book_seq_last_reason == "ok"
    assert rt.book_missing_seq_last_gap == 0
    assert rt.book_seq_last_u == 200
    assert rt.book_resync_requested is False


def test_gap_step_no_threshold():
    """Gap of 11 with EMA threshold=0 (disabled) -> resync NOT triggered."""
    rt = FakeRuntime()
    rt.book_seq_last_u = 150
    # Gap: U=162 > prev_u+1=151 → gap count = 162-151 = 11
    _update_book_seq_uu_dq(rt, ingest_ts_ms=3000, book_raw={"U": 162, "u": 200})
    assert rt.book_seq_last_reason == "gap"
    assert rt.book_missing_seq_last_gap == 11
    # EMA threshold=0 (default) means resync path is disabled
    assert rt.book_resync_requested is False


def test_gap_step_with_threshold_triggers_resync():
    """Gap + threshold set + prev EMA elevated -> resync armed."""
    rt = FakeRuntime()
    rt.book_seq_last_u = 150
    rt.book_missing_seq_ema = 0.5  # pre-set elevated EMA
    rt.config = {
        "book_resync_ema_threshold": 0.3,  # EMA=0.5 >= thr=0.3 → arm
        "book_resync_cooldown_ms": 0,       # no cooldown
        "book_gap_min_missing_updates": 1,
    }
    _update_book_seq_uu_dq(rt, ingest_ts_ms=4000, book_raw={"U": 162, "u": 200})
    assert rt.book_seq_last_reason == "gap"
    assert rt.book_resync_requested is True


def test_dup_step_does_not_advance_seq():
    """Duplicate (cur_u <= prev_u) -> reason=dup, seq state unchanged."""
    rt = FakeRuntime()
    rt.book_seq_last_u = 200
    _update_book_seq_uu_dq(rt, ingest_ts_ms=5000, book_raw={"U": 190, "u": 200})
    assert rt.book_seq_last_reason == "dup"
    assert rt.book_seq_last_u == 200  # must not advance


def test_missing_u_field_returns_early():
    """No U/u fields -> function returns silently (partial depth case)."""
    rt = FakeRuntime()
    rt.book_seq_last_u = 100
    _update_book_seq_uu_dq(rt, ingest_ts_ms=6000, book_raw={"bids": [], "asks": []})
    assert rt.book_seq_last_u == 100  # untouched
    assert rt.book_seq_last_reason == ""
