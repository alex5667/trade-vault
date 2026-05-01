from orderflow_services.enforce_bucket_promoter_rollback_controller_v1 import BucketStats, decide_rollback


def test_decide_rollback_triggers_on_p95():
    stats = {
        "HIGH_VOL": BucketStats(bucket="HIGH_VOL", db_n=200, resid_p95=6.0, resid_p99=9.0, edge_neg_share=0.1)
    }
    dec = decide_rollback(
        added_buckets=["HIGH_VOL"],
        stats_by_bucket=stats,
        min_db_n=80,
        max_p95=5.0,
        max_p99=12.0,
        max_edge_neg_share=0.35,
        target_slip="HIGH_VOL_LOW_LIQ",
        target_taker="HIGH_VOL_LOW_LIQ",
    )
    assert dec.rollback is True
    assert any("p95_high" in r for r in dec.reasons)


def test_decide_rollback_skips_low_n():
    stats = {
        "HIGH_VOL": BucketStats(bucket="HIGH_VOL", db_n=20, resid_p95=100.0, resid_p99=100.0, edge_neg_share=1.0)
    }
    dec = decide_rollback(
        added_buckets=["HIGH_VOL"],
        stats_by_bucket=stats,
        min_db_n=80,
        max_p95=5.0,
        max_p99=12.0,
        max_edge_neg_share=0.35,
        target_slip="A",
        target_taker="B",
    )
    assert dec.rollback is False


def test_decide_rollback_triggers_on_edge_neg():
    stats = {
        "LOW_LIQ": BucketStats(bucket="LOW_LIQ", db_n=200, resid_p95=1.0, resid_p99=2.0, edge_neg_share=0.5)
    }
    dec = decide_rollback(
        added_buckets=["LOW_LIQ"],
        stats_by_bucket=stats,
        min_db_n=80,
        max_p95=5.0,
        max_p99=12.0,
        max_edge_neg_share=0.35,
        target_slip="A",
        target_taker="B",
    )
    assert dec.rollback is True
    assert any("edge_neg_high" in r for r in dec.reasons)
