from __future__ import annotations

from core.book_seq_tracker_uu import decide_book_seq_uu, ema_update_clamped


def test_decide_book_seq_init_ok_overlap_gap_dup():
    # no seq fields
    d = decide_book_seq_uu(prev_u=10, cur_U=0, cur_u=0)
    assert d.has_seq_fields is False
    assert d.reason == "no_seq_fields"

    # init
    d = decide_book_seq_uu(prev_u=0, cur_U=157, cur_u=160)
    assert d.has_seq_fields is True
    assert d.reason == "init"
    assert d.next_last_u == 160

    # ok
    d = decide_book_seq_uu(prev_u=160, cur_U=161, cur_u=165)
    assert d.reason == "ok"
    assert d.gap == 0

    # overlap: U < prev_u+1 <= u
    d = decide_book_seq_uu(prev_u=165, cur_U=164, cur_u=170)
    assert d.reason == "overlap"

    # gap
    d = decide_book_seq_uu(prev_u=170, cur_U=175, cur_u=180)
    assert d.reason == "gap"
    assert d.gap == 4
    assert d.missing_event == 1.0

    # dup
    d = decide_book_seq_uu(prev_u=180, cur_U=170, cur_u=175)
    assert d.reason == "dup"


def test_ema_update_clamped():
    assert ema_update_clamped(0.0, 1.0, 0.1) == 0.1
    assert ema_update_clamped(0.2, 0.0, 0.5) == 0.1
    assert ema_update_clamped(0.2, 0.0, 0.0) == 0.2
    assert ema_update_clamped(0.2, 0.0, 1.0) == 0.0
