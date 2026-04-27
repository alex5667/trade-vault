from __future__ import annotations


from core.dq_observe_only import apply_observe_only_book_veto


def test_observe_only_blocks_when_disabled():
    out = apply_observe_only_book_veto(
        dq_level=2,
        dq_veto=1,
        dq_reason_bucket="book_seq",
        dq_reasons=["book_seq_hard"],
        uptime_sec=999999.0,
        cfg={"dq_book_veto_enabled": False, "dq_observe_only_sec": 86400},
    )
    assert out.dq_veto == 0
    assert out.suppressed is True
    assert out.suppress_reason == "book_veto_disabled"


def test_observe_only_blocks_before_window_even_if_enabled():
    out = apply_observe_only_book_veto(
        dq_level=2,
        dq_veto=1,
        dq_reason_bucket="book_seq",
        dq_reasons=["book_missing_seq_hard"],
        uptime_sec=100.0,
        cfg={"dq_book_veto_enabled": True, "dq_observe_only_sec": 86400},
    )
    assert out.dq_veto == 0
    assert out.suppressed is True
    assert out.suppress_reason == "observe_only"


def test_veto_enabled_after_window():
    out = apply_observe_only_book_veto(
        dq_level=2,
        dq_veto=1,
        dq_reason_bucket="book_seq",
        dq_reasons=["book_seq"],
        uptime_sec=86400.0,
        cfg={"dq_book_veto_enabled": True, "dq_observe_only_sec": 86400},
    )
    assert out.dq_veto == 1
    assert out.suppressed is False


def test_non_book_veto_is_never_suppressed():
    out = apply_observe_only_book_veto(
        dq_level=2,
        dq_veto=1,
        dq_reason_bucket="tick_seq",
        dq_reasons=["tick_seq_hard"],
        uptime_sec=0.0,
        cfg={"dq_book_veto_enabled": False, "dq_observe_only_sec": 86400},
    )
    assert out.dq_veto == 1
    assert out.suppressed is False


def test_soft_or_no_veto_is_unchanged():
    out1 = apply_observe_only_book_veto(
        dq_level=1,
        dq_veto=0,
        dq_reason_bucket="book_seq",
        dq_reasons=["book_seq_soft"],
        uptime_sec=0.0,
        cfg={"dq_book_veto_enabled": True, "dq_observe_only_sec": 86400},
    )
    assert out1.dq_veto == 0
    assert out1.suppressed is False

    out2 = apply_observe_only_book_veto(
        dq_level=2,
        dq_veto=0,
        dq_reason_bucket="book_seq",
        dq_reasons=["book_seq_hard"],
        uptime_sec=0.0,
        cfg={"dq_book_veto_enabled": True, "dq_observe_only_sec": 86400},
    )
    assert out2.dq_veto == 0
    assert out2.suppressed is False
