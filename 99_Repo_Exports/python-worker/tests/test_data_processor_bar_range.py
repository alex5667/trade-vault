import logging
from collections import deque
from types import SimpleNamespace

from contexts import BucketState
from handlers.data_processor import OrderFlowDataProcessor


def _mk_proc():
    # Создаём объект без тяжёлого __init__ (чтобы тест был изолированным)
    p = OrderFlowDataProcessor.__new__(OrderFlowDataProcessor)
    # Use actual BucketState dataclass (fields are now guaranteed to exist)
    p._bucket_state = BucketState()
    p.logger = logging.getLogger("test")
    p.config = SimpleNamespace()

    # Init internal robust tracker state
    p._bar_tf_ms = 60_000
    p._bar_id = None
    p._bar_open = 0.0
    p._bar_high = 0.0
    p._bar_low = 0.0
    p._bar_last_ts_ms = 0
    p._bar_range_alpha = 0.1
    p._bar_range_bps_ema = 0.0
    p._bar_range_hist_bps = deque(maxlen=20)
    return p

def test_bar_init_and_update_same_bar():
    p = _mk_proc()
    t0 = 1_700_000_000_000
    p._update_bar_range(100.0, t0)
    assert p._bucket_state.bar_open == 100.0
    assert p._bucket_state.bar_high == 100.0
    assert p._bucket_state.bar_low == 100.0

    p._update_bar_range(105.0, t0 + 1_000)
    assert p._bucket_state.bar_high == 105.0
    assert p._bucket_state.bar_low == 100.0
    assert p._bucket_state.bar_range == 5.0
    assert p._bucket_state.bar_range_bps > 0.0

def test_bar_rollover_finalizes_prev():
    p = _mk_proc()
    t0 = 1_700_000_000_000
    p._update_bar_range(100.0, t0)
    p._update_bar_range(110.0, t0 + 10_000)   # high
    p._update_bar_range(90.0,  t0 + 20_000)   # low -> range=20

    # first tick of next minute
    t1 = (t0 // 60_000 + 1) * 60_000
    p._update_bar_range(95.0, t1)

    # Check history was updated
    assert len(p._bar_range_hist_bps) == 1
    # Check prev fields are populated (fields are now guaranteed to exist)
    # prev_range_bps logic: range=20, open=100 -> 2000 bps
    assert p._bucket_state.prev_bar_range_bps > 0.0

    # new bar reset
    assert p._bucket_state.bar_open == 95.0
    assert p._bucket_state.bar_high == 95.0
    assert p._bucket_state.bar_low == 95.0

def test_time_backwards_is_ignored():
    p = _mk_proc()
    t0 = 1_700_000_000_000
    p._update_bar_range(100.0, t0 + 10_000)
    # older timestamp by >500ms => ignored
    p._update_bar_range(200.0, t0)
    # high should remain 100.0, not jump to 200.0
    assert p._bucket_state.bar_high == 100.0
    assert p._bucket_state.bar_time_backwards_cnt >= 1
    assert p._bucket_state.bar_time_backwards_flag is True

def test_robust_z_score_calculation():
    p = _mk_proc()
    # Populate history with stable range bps (e.g. 10.0)
    # Median = 10.0, MAD = 0 -> Z will be 0 on exact match, or high on deviation
    # We need to fill enough history (>=10)
    for i in range(15):
        p._bar_range_hist_bps.append(10.0)

    # Check internal helper logic (indirectly via update)
    # If current range bps is huge, Z should be high?
    # BUT: MAD of [10, 10...] is 0. Division by zero protection -> returns 0.0.

    # Let's create some variance
    p._bar_range_hist_bps.clear()
    vals = [10.0, 12.0, 8.0, 11.0, 9.0] * 3 # 15 vals, median~10, mad~1
    for v in vals:
        p._bar_range_hist_bps.append(v)

    t0 = 1_800_000_000_000
    # price=100. T=0.
    p._update_bar_range(100.0, t0)
    # Move price to create large range. +200 range -> 20000 bps (huge)
    p._update_bar_range(300.0, t0 + 1000)

    # Z should be non-zero (positive)
    assert p._bucket_state.bar_range_z > 2.0

def test_bar_fields_present_and_populated():
    p = _mk_proc()  # как раньше
    t0 = 1_700_000_000_000
    p._update_bar_range(100.0, t0)
    assert p._bucket_state.bar_open == 100.0
    assert p._bucket_state.bar_range_bps_ema >= 0.0
