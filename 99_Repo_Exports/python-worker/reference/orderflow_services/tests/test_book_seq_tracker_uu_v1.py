from services.orderflow.components.book_seq_tracker_uu import (
    decide_book_seq_uu,
    resolve_book_seq_ema_alpha,
)


def test_book_seq_continuity_with_Uu_ok_and_overlap() -> None:
    # ok: U == prev_u + 1
    dec = decide_book_seq_uu(prev_u=160, cur_U=161, cur_u=165)
    assert dec.reason in ("ok",)
    assert dec.missing_event == 0.0

    # overlap: U < prev_u + 1 <= u
    dec = decide_book_seq_uu(prev_u=160, cur_U=150, cur_u=165)
    assert dec.reason == "overlap"
    assert dec.missing_event == 0.0


def test_book_seq_gap_detected() -> None:
    dec = decide_book_seq_uu(prev_u=160, cur_U=170, cur_u=175)
    assert dec.reason == "gap"
    assert dec.gap == 9
    assert dec.missing_event == 1.0


def test_book_seq_dup_old() -> None:
    dec = decide_book_seq_uu(prev_u=200, cur_U=190, cur_u=195)
    assert dec.reason == "dup"
    assert dec.missing_event == 0.0


def test_book_seq_init() -> None:
    dec = decide_book_seq_uu(prev_u=0, cur_U=161, cur_u=165)
    assert dec.reason == "init"
    assert dec.missing_event == 0.0


def test_book_seq_alpha_mapping_defaults() -> None:
    assert resolve_book_seq_ema_alpha({"book_stream_interval_ms": 100}) == 0.10
    assert resolve_book_seq_ema_alpha({"book_stream_interval_ms": 250}) == 0.20
    assert resolve_book_seq_ema_alpha({"book_stream_interval_ms": 500}) == 0.30
    # 1Hz default is chosen inside the allowed range 0.30–0.50
    a = resolve_book_seq_ema_alpha({"book_stream_interval_ms": 1000})
    assert 0.30 <= a <= 0.50


def test_book_seq_alpha_explicit_override() -> None:
    assert resolve_book_seq_ema_alpha({"dq_book_seq_ema_alpha": 0.07, "book_stream_interval_ms": 100}) == 0.07
