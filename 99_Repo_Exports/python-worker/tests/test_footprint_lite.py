from __future__ import annotations

from core.footprint_lite import FootprintLite


def test_bucket_aggregation_and_imbalance():
    fp = FootprintLite(bucket_px=1.0, max_buckets=10)
    # bucket 100: heavy buy
    fp.update(price=100.2, qty=5.0, signed_qty=+5.0)
    fp.update(price=100.4, qty=5.0, signed_qty=+5.0)
    # bucket 101: mixed
    fp.update(price=101.1, qty=2.0, signed_qty=-2.0)
    fp.update(price=101.2, qty=1.0, signed_qty=+1.0)

    snap = fp.finalize(
        bar_open=100.0, bar_close=100.1, bar_high=101.5, bar_low=99.5,
        bar_delta_sum=+9.0, bar_vol=13.0,
    )
    assert snap.n_buckets >= 2
    assert snap.max_imbalance > 0.6
    assert snap.peak_delta > 0  # peak bucket is buy-dominant


def test_absorption_bias_long_on_sell_aggression_low_progress():
    fp = FootprintLite(bucket_px=1.0, max_buckets=20)
    # simulate strong sell aggression clustered, but bar closes flat/up
    for _ in range(10):
        fp.update(price=100.0, qty=1.0, signed_qty=-1.0)  # sells

    snap = fp.finalize(
        bar_open=100.0, bar_close=100.05, bar_high=100.2, bar_low=99.8,
        bar_delta_sum=-10.0, bar_vol=10.0,
    )
    # low progress (close ~ open), sell aggression => absorption buyers => LONG bias
    assert snap.absorption_bias == "LONG"
    assert snap.absorb_score > 0.0


def test_lru_eviction_is_bounded():
    # max_buckets is clamped to 16 in FootprintLite
    fp = FootprintLite(bucket_px=1.0, max_buckets=16)
    # create many distinct buckets
    for i in range(40):
        fp.update(price=100.0 + i, qty=1.0, signed_qty=+1.0)
    assert fp.evictions > 0
    snap = fp.finalize(
        bar_open=100.0, bar_close=139.0, bar_high=140.0, bar_low=99.0,
        bar_delta_sum=+40.0, bar_vol=40.0,
    )
    assert snap.n_buckets <= 16
