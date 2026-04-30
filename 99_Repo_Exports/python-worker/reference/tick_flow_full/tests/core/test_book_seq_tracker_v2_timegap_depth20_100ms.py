"""Unit tests for BookSeqTracker v2 — timegap path used for @depth20@100ms.

Tests cover:
  - time-gap path (no U field): init, normal, gap, jitter suppression
  - strict U/u path: missing-seq, dup, reorder, overlap
  - edge cases: bad timestamps, zero expected_interval_ms
"""
import os
import sys
from pathlib import Path

# Add tick_flow_full to sys.path so 'core.*' is importable.
ROOT = Path(__file__).resolve().parents[1]  # tests/
TICK_FLOW_FULL = ROOT.parent  # tick_flow_full/
if str(TICK_FLOW_FULL) not in sys.path:
    sys.path.insert(0, str(TICK_FLOW_FULL))

from core.book_seq_tracker_v2 import compute_book_seq_update


def test_timegap_depth20_100ms_marks_gap_after_min_missing_updates():
    """depth20@100ms nominal stream interval — large dt must flag gap."""
    exp_ms = 100
    min_missing = 3

    # First message initializes state
    upd0 = compute_book_seq_update(
        prev_last_u=0
        prev_ingest_ts_ms=0
        payload={"u": 100}
        ingest_ts_ms=1_000
        expected_interval_ms=exp_ms
        min_missing_updates=min_missing
    )
    assert upd0.reason == "init"
    assert upd0.is_gap_event is False

    # Normal cadence: dt=100ms => missing_est=0
    upd1 = compute_book_seq_update(
        prev_last_u=upd0.last_u
        prev_ingest_ts_ms=upd0.last_ingest_ts_ms
        payload={"u": 101}
        ingest_ts_ms=1_100
        expected_interval_ms=exp_ms
        min_missing_updates=min_missing
    )
    assert upd1.reason == "ok"
    assert upd1.gap_missing_updates == 0
    assert upd1.is_gap_event is False

    # Large pause: dt=600ms => floor(6)-1=5 missing => gap
    upd2 = compute_book_seq_update(
        prev_last_u=upd1.last_u
        prev_ingest_ts_ms=upd1.last_ingest_ts_ms
        payload={"u": 102}
        ingest_ts_ms=1_700
        expected_interval_ms=exp_ms
        min_missing_updates=min_missing
    )
    assert upd2.reason == "gap"
    assert upd2.gap_missing_updates == 5
    assert upd2.is_gap_event is True


def test_strict_Uu_path_detects_exact_missing_seq():
    """Strict path: U/u present => exact missing seq count."""
    upd0 = compute_book_seq_update(
        prev_last_u=150
        prev_ingest_ts_ms=1_000
        payload={"U": 152, "u": 155}
        ingest_ts_ms=1_100
        expected_interval_ms=100
        min_missing_updates=3
    )
    # Missing: U=152, prev=150 => gap=1
    assert upd0.reason == "gap"
    assert upd0.gap_missing_updates == 1
    assert upd0.is_gap_event is True
    assert upd0.last_u == 155


def test_timegap_does_not_flag_small_jitter_as_gap():
    """Small dt jitter below min_missing_updates threshold must NOT be a gap."""
    upd0 = compute_book_seq_update(
        prev_last_u=0
        prev_ingest_ts_ms=0
        payload={"u": 100}
        ingest_ts_ms=1_000
        expected_interval_ms=100
        min_missing_updates=3
    )

    # dt=250ms => floor(2)-1=1 missing, but min_missing_updates=3 => no gap
    upd1 = compute_book_seq_update(
        prev_last_u=upd0.last_u
        prev_ingest_ts_ms=upd0.last_ingest_ts_ms
        payload={"u": 101}
        ingest_ts_ms=1_250
        expected_interval_ms=100
        min_missing_updates=3
    )
    assert upd1.reason == "ok"
    assert upd1.gap_missing_updates == 1
    assert upd1.is_gap_event is False


def test_strict_Uu_dup():
    """Duplicate message (u <= prev_last_u) must NOT count as gap."""
    upd = compute_book_seq_update(
        prev_last_u=200
        prev_ingest_ts_ms=1_000
        payload={"U": 198, "u": 200}
        ingest_ts_ms=1_100
        expected_interval_ms=100
        min_missing_updates=1
    )
    assert upd.reason == "dup"
    assert upd.is_gap_event is False
    assert upd.gap_missing_updates == 0


def test_strict_Uu_reorder():
    """Out-of-order message (u < prev_last_u) must NOT count as gap."""
    upd = compute_book_seq_update(
        prev_last_u=200
        prev_ingest_ts_ms=1_000
        payload={"U": 198, "u": 199}
        ingest_ts_ms=1_100
        expected_interval_ms=100
        min_missing_updates=1
    )
    assert upd.reason == "reorder"
    assert upd.is_gap_event is False


def test_strict_Uu_overlap_normal():
    """Normal overlap (U == prev+1) must reason='ok', no gap."""
    upd = compute_book_seq_update(
        prev_last_u=100
        prev_ingest_ts_ms=1_000
        payload={"U": 101, "u": 105}
        ingest_ts_ms=1_100
        expected_interval_ms=100
        min_missing_updates=1
    )
    assert upd.reason == "ok"
    assert upd.is_gap_event is False
    assert upd.last_u == 105


def test_timegap_backwards_clock_not_gap():
    """Clock jump backwards must NOT count as gap (reorder path)."""
    upd = compute_book_seq_update(
        prev_last_u=50
        prev_ingest_ts_ms=2_000
        payload={"u": 51}
        ingest_ts_ms=1_900,  # backwards
        expected_interval_ms=100
        min_missing_updates=3
    )
    assert upd.reason == "reorder"
    assert upd.is_gap_event is False
    assert upd.gap_missing_updates == 0


def test_timegap_zero_expected_interval_no_gap():
    """If expected_interval_ms=0, fallback path must not flag gap."""
    upd = compute_book_seq_update(
        prev_last_u=50
        prev_ingest_ts_ms=1_000
        payload={"u": 55}
        ingest_ts_ms=5_000
        expected_interval_ms=0
        min_missing_updates=1
    )
    assert upd.reason == "no_interval"
    assert upd.is_gap_event is False


def test_timegap_lastUpdateId_regression_not_gap():
    """In partial-depth path, if u < prev_last_u, it is out-of-order data — not a gap."""
    upd = compute_book_seq_update(
        prev_last_u=200
        prev_ingest_ts_ms=1_000
        payload={"u": 195},  # regression
        ingest_ts_ms=1_600,  # large dt, would normally flag gap
        expected_interval_ms=100
        min_missing_updates=3
    )
    # regression suppresses gap
    assert upd.reason == "reorder"
    assert upd.is_gap_event is False
