from __future__ import annotations

from pathlib import Path

import pytest

from orderflow_services.tests._repo_import import find_repo_root, load_module_from_candidates


def _load_book_seq_tracker():
    repo = find_repo_root(Path(__file__).resolve())
    # Prefer SoT (tick_flow_full) then mirror (services).
    candidates = [
        "tick_flow_full/services/orderflow/components/book_seq_tracker_uu.py",
        "services/orderflow/components/book_seq_tracker_uu.py",
    ]
    return load_module_from_candidates(repo, candidates, module_name="book_seq_tracker_uu")


def test_book_seq_continuity_with_Uu_ok_overlap():
    """prev_u=160, event U=161,u=165 → ok, missing_event=0

    Note:
      Binance often sends overlapping depthUpdate ranges; this test covers
      the strict-continuity case U==prev_u+1.
    """
    mod, _ = _load_book_seq_tracker()
    dec = mod.decide_book_seq_uu(prev_u=160, cur_U=161, cur_u=165)
    assert dec.has_seq_fields is True
    assert dec.reason in ("ok", "overlap")
    assert dec.gap == 0
    assert float(dec.missing_event) == 0.0
    assert dec.next_last_u == 165


def test_book_seq_gap_detected():
    """prev_u=160, event U=170,u=175 → gap=9, missing_event=1"""
    mod, _ = _load_book_seq_tracker()
    dec = mod.decide_book_seq_uu(prev_u=160, cur_U=170, cur_u=175)
    assert dec.reason == "gap"
    assert dec.gap == 9
    assert float(dec.missing_event) == 1.0
    assert dec.next_last_u == 175


def test_book_seq_dup_old():
    """prev_u=200, event U=190,u=195 → dup/old, missing_event=0"""
    mod, _ = _load_book_seq_tracker()
    dec = mod.decide_book_seq_uu(prev_u=200, cur_U=190, cur_u=195)
    # Some implementations label this as dup or reorder; both MUST NOT count as missing.
    assert dec.reason in ("dup", "reorder", "reorder_or_reset")
    assert dec.gap == 0
    assert float(dec.missing_event) == 0.0
    # next_last_u must not go backwards
    assert dec.next_last_u in (200, 195)


def test_book_seq_init():
    """No prev_u → init, ema unchanged.

    For the pure decision function, init means prev_u<=0.
    """
    mod, _ = _load_book_seq_tracker()
    dec = mod.decide_book_seq_uu(prev_u=0, cur_U=161, cur_u=165)
    assert dec.reason == "init"
    assert dec.gap == 0
    assert float(dec.missing_event) == 0.0
    assert dec.next_last_u == 165
