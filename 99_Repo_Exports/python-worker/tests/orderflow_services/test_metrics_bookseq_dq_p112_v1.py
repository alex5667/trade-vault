from __future__ import annotations


def test_sanitize_dq_bucket_is_finite_set() -> None:
    from services.orderflow.metrics_bookseq_dq_p112 import sanitize_dq_bucket

    assert sanitize_dq_bucket(None) == "other"
    assert sanitize_dq_bucket("") == "other"
    assert sanitize_dq_bucket("book_seq") == "book_seq"
    assert sanitize_dq_bucket("tick_seq") == "tick_seq"
    assert sanitize_dq_bucket("gap") == "gap_p95"
    assert sanitize_dq_bucket("tick_gap_p95") == "gap_p95"
    assert sanitize_dq_bucket("data_health") == "data_health"
    # Unknown values must collapse into 'other' to avoid high-cardinality.
    assert sanitize_dq_bucket("some_new_reason") == "other"



def test_emit_dq_metrics_is_fail_open() -> None:
    from services.orderflow.metrics_bookseq_dq_p112 import emit_dq_metrics

    # Must not raise.
    emit_dq_metrics(symbol="BTCUSDT", dq_level=2, dq_veto=1, bucket="gap")
