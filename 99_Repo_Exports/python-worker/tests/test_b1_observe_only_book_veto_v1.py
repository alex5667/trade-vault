from core.core_snapshot.dq_observe_only import apply_observe_only_book_veto


def test_observe_only_blocks_veto_during_warmup():
    # When enabled, and uptime < warmup, veto=0 is returned and suppressed=True
    decision = apply_observe_only_book_veto(
        dq_level=2,
        dq_veto=1,
        dq_reason_bucket="book_seq",
        dq_reasons=["book_seq_hard"],
        uptime_sec=10.0,
        cfg={"dq_book_veto_enabled": True, "dq_observe_only_sec": 86400}
    )
    assert decision.dq_veto == 0
    assert decision.suppressed is True
    assert decision.suppress_reason == "observe_only"

def test_observe_only_allows_veto_after_warmup():
    decision = apply_observe_only_book_veto(
        dq_level=2,
        dq_veto=1,
        dq_reason_bucket="book_seq",
        dq_reasons=["book_seq_hard"],
        uptime_sec=90000.0,
        cfg={"dq_book_veto_enabled": True, "dq_observe_only_sec": 86400}
    )
    assert decision.dq_veto == 1
    assert decision.suppressed is False
    assert decision.suppress_reason is None

def test_veto_suppressed_when_disabled():
    decision = apply_observe_only_book_veto(
        dq_level=2,
        dq_veto=1,
        dq_reason_bucket="book_seq",
        dq_reasons=["book_seq_hard"],
        uptime_sec=90000.0,
        cfg={"dq_book_veto_enabled": False, "dq_observe_only_sec": 86400}
    )
    assert decision.dq_veto == 0
    assert decision.suppressed is True
    assert decision.suppress_reason == "book_veto_disabled"

def test_does_not_suppress_non_book_vetos():
    decision = apply_observe_only_book_veto(
        dq_level=2,
        dq_veto=1,
        dq_reason_bucket="data_health",
        dq_reasons=["low_data_health"],
        uptime_sec=10.0,
        cfg={"dq_book_veto_enabled": True, "dq_observe_only_sec": 86400}
    )
    assert decision.dq_veto == 1
    assert decision.suppressed is False

def test_does_not_affect_lower_levels():
    decision = apply_observe_only_book_veto(
        dq_level=1,
        dq_veto=0,
        dq_reason_bucket="book_seq",
        dq_reasons=["book_stale"],
        uptime_sec=10.0,
        cfg={"dq_book_veto_enabled": True, "dq_observe_only_sec": 86400}
    )
    assert decision.dq_veto == 0
    assert decision.suppressed is False
