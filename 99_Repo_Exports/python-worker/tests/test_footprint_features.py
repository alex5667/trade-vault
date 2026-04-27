from __future__ import annotations

from core.footprint_features import (
    compute_bucket_stats, compute_edge_ladders, compute_poc, poc_on_edge, compute_eff_delta
)


import pytest

def test_bucket_stats_imbalance():
    m = {
        10: (3.0, 1.0),  # 3:1 buy dominant
        11: (1.0, 4.0),  # 4:1 sell dominant
    }
    keys, st = compute_bucket_stats(m, eps=1e-9)
    assert keys == [10, 11]
    assert st[10]["imb_ratio"] == pytest.approx(3.0)
    assert st[10]["dom"] == 1.0
    assert st[11]["imb_ratio"] == pytest.approx(4.0)
    assert st[11]["dom"] == -1.0


def test_ladder_low_sell():
    # three consecutive buckets near low, sell dominates 3:1+
    m = {
        10: (1.0, 4.0),
        11: (1.0, 4.0),
        12: (1.0, 4.0),
        20: (4.0, 1.0), # Buy dominant at high edge
    }
    keys, st = compute_bucket_stats(m, eps=1e-9)
    low_len, high_len = compute_edge_ladders(keys, st, ratio_th=3.0, edge_buckets=4)
    assert low_len == 3
    assert high_len == 1 # Correct: bucket 20 is at top and is buy-dom 4:1


def test_ladder_high_buy():
    # two consecutive buckets at top, buy dominates 3:1+
    m = {
        100: (1.0, 1.0),
        110: (5.0, 1.0),
        111: (5.0, 1.0),
    }
    keys, st = compute_bucket_stats(m, eps=1e-9)
    low_len, high_len = compute_edge_ladders(keys, st, ratio_th=3.0, edge_buckets=4)
    assert low_len == 0
    assert high_len == 2


def test_poc_on_edge():
    # POC is at the bottom
    m = {
        10: (10.0, 10.0), # POC total=20
        11: (1.0, 1.0),
        12: (1.0, 1.0),
    }
    keys, st = compute_bucket_stats(m, eps=1e-9)
    poc_b, poc_tot = compute_poc(keys, st)
    assert poc_b == 10
    on, side = poc_on_edge(poc_bucket=poc_b, keys=keys, edge_tol_buckets=1)
    assert on == 1
    assert side == "LOW"


def test_eff_delta():
    # 10 ticks moved (0.1 / 0.01), delta 1000
    eff = compute_eff_delta(
        bar_open=100.0, 
        bar_close=100.1, 
        bucket_px=0.01, 
        bar_delta_sum=1000.0, 
        eps=1e-9
    )
    # 10 / 1000 = 0.01
    assert abs(eff - 0.01) < 1e-7
