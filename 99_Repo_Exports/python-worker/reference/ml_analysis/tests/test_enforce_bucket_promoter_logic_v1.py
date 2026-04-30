# -*- coding: utf-8 -*-
# P89: all BucketHealth constructors now require edge_neg_share; all decide calls require max_edge_neg_share
from ml_analysis.tools.nightly_enforce_bucket_promoter_v1 import (
    BucketHealth
    decide_next_allowlist
)


def test_decide_promotes_high_vol_first():
    """Bucket with healthy edge_neg_share should be promoted."""
    health = {
        "HIGH_VOL_LOW_LIQ": BucketHealth("HIGH_VOL_LOW_LIQ", db_n=500, resid_p95=1.0, resid_p99=3.0, edge_neg_share=0.10, eligible_n=1000, ok_soft_rate=0.20)
        "HIGH_VOL": BucketHealth("HIGH_VOL", db_n=400, resid_p95=1.5, resid_p99=4.0, edge_neg_share=0.10, eligible_n=800, ok_soft_rate=0.15)
        "LOW_LIQ": BucketHealth("LOW_LIQ", db_n=400, resid_p95=1.0, resid_p99=4.0, edge_neg_share=0.10, eligible_n=800, ok_soft_rate=0.15)
    }
    dec = decide_next_allowlist(
        current_allow="HIGH_VOL_LOW_LIQ"
        health_by_bucket=health
        promote_order=["HIGH_VOL", "LOW_LIQ"]
        default_bucket="HIGH_VOL_LOW_LIQ"
        min_db_n=100
        max_p95=3.0
        max_p99=8.0
        max_edge_neg_share=0.40
        min_eligible_n=200
        min_ok_soft_rate=0.05
    )
    assert dec.ok is True
    assert dec.added_bucket == "HIGH_VOL"
    assert dec.new_allowlist == "HIGH_VOL_LOW_LIQ,HIGH_VOL"


def test_decide_skips_when_residual_high():
    """Bucket with high residual p95 must not be promoted."""
    health = {
        "HIGH_VOL_LOW_LIQ": BucketHealth("HIGH_VOL_LOW_LIQ", db_n=500, resid_p95=1.0, resid_p99=3.0, edge_neg_share=0.10, eligible_n=1000, ok_soft_rate=0.20)
        "HIGH_VOL": BucketHealth("HIGH_VOL", db_n=400, resid_p95=5.0, resid_p99=9.0, edge_neg_share=0.10, eligible_n=800, ok_soft_rate=0.15)
    }
    dec = decide_next_allowlist(
        current_allow="HIGH_VOL_LOW_LIQ"
        health_by_bucket=health
        promote_order=["HIGH_VOL"]
        default_bucket="HIGH_VOL_LOW_LIQ"
        min_db_n=100
        max_p95=3.0
        max_p99=8.0
        max_edge_neg_share=0.40
        min_eligible_n=200
        min_ok_soft_rate=0.05
    )
    assert dec.ok is False
    assert dec.new_allowlist == "HIGH_VOL_LOW_LIQ"


def test_decide_skips_when_gate_n_low():
    """Bucket with insufficient gate samples must not be promoted."""
    health = {
        "HIGH_VOL": BucketHealth("HIGH_VOL", db_n=400, resid_p95=1.0, resid_p99=2.0, edge_neg_share=0.10, eligible_n=50, ok_soft_rate=0.20)
    }
    dec = decide_next_allowlist(
        current_allow="HIGH_VOL_LOW_LIQ"
        health_by_bucket=health
        promote_order=["HIGH_VOL"]
        default_bucket="HIGH_VOL_LOW_LIQ"
        min_db_n=100
        max_p95=3.0
        max_p99=8.0
        max_edge_neg_share=0.40
        min_eligible_n=200
        min_ok_soft_rate=0.05
    )
    assert dec.ok is False


def test_decide_skips_when_edge_neg_share_high():
    """P89: bucket with edge_neg_share above threshold must not be promoted."""
    health = {
        "HIGH_VOL_LOW_LIQ": BucketHealth("HIGH_VOL_LOW_LIQ", db_n=500, resid_p95=1.0, resid_p99=3.0, edge_neg_share=0.10, eligible_n=1000, ok_soft_rate=0.20)
        # edge_neg_share=0.55 > max_edge_neg_share=0.40 => should be blocked
        "HIGH_VOL": BucketHealth("HIGH_VOL", db_n=400, resid_p95=1.5, resid_p99=4.0, edge_neg_share=0.55, eligible_n=800, ok_soft_rate=0.15)
    }
    dec = decide_next_allowlist(
        current_allow="HIGH_VOL_LOW_LIQ"
        health_by_bucket=health
        promote_order=["HIGH_VOL"]
        default_bucket="HIGH_VOL_LOW_LIQ"
        min_db_n=100
        max_p95=3.0
        max_p99=8.0
        max_edge_neg_share=0.40
        min_eligible_n=200
        min_ok_soft_rate=0.05
    )
    assert dec.ok is False
    # Reason must mention edge_neg_high
    reasons_str = " ".join(dec.reasons)
    assert "edge_neg_high" in reasons_str


def test_decide_allows_when_edge_neg_share_at_threshold():
    """P89: edge_neg_share exactly at threshold (not exceeding) should allow promotion."""
    health = {
        "HIGH_VOL_LOW_LIQ": BucketHealth("HIGH_VOL_LOW_LIQ", db_n=500, resid_p95=1.0, resid_p99=3.0, edge_neg_share=0.10, eligible_n=1000, ok_soft_rate=0.20)
        # edge_neg_share=0.40 == max_edge_neg_share=0.40 — boundary: NOT blocked (condition is >, not >=)
        "HIGH_VOL": BucketHealth("HIGH_VOL", db_n=400, resid_p95=1.5, resid_p99=4.0, edge_neg_share=0.40, eligible_n=800, ok_soft_rate=0.15)
    }
    dec = decide_next_allowlist(
        current_allow="HIGH_VOL_LOW_LIQ"
        health_by_bucket=health
        promote_order=["HIGH_VOL"]
        default_bucket="HIGH_VOL_LOW_LIQ"
        min_db_n=100
        max_p95=3.0
        max_p99=8.0
        max_edge_neg_share=0.40
        min_eligible_n=200
        min_ok_soft_rate=0.05
    )
    assert dec.ok is True
    assert dec.added_bucket == "HIGH_VOL"
