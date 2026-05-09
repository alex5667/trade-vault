from unittest.mock import MagicMock

from contexts import Tick
from core.robust_stats import RollingRobustZ
from handlers.data_processor import OrderFlowDataProcessor


# Mock config
class MockConfig:
    delta_window_ticks = 100
    delta_bucket_ms = 1000
    timeframe_s = 60
    regime_label_hi = 0.35
    regime_label_lo = -0.35
    regime_atr_hi_q = 0.70
    regime_atr_lo_q = 0.35
    regime_adx_hi_q = 0.75
    regime_adx_lo_q = 0.40
    regime_ping_scale = 0.20
    regime_delta_scale = 1.0
    regime_w_atr = 0.35
    regime_w_adx = 0.20
    regime_w_delta = 0.25
    regime_w_hold = 0.25
    regime_w_ping = 0.15
    regime_trend_dir_hold_min = 0.10

class MockSpecs:
    pass

def test_rolling_robust_z_basic():
    rz = RollingRobustZ(window=10)

    # Push constant values (need >= 8 for valid Z)
    for _ in range(8):
        rz.update(10.0)

    # Still 0 variance -> Z=0 or bounded
    assert rz.z(10.0) == 0.0

    # Push outlier
    # MAD will be 0, so Z uses epsilon -> huge value
    z = rz.z(20.0)
    assert abs(z) > 1000 # essentially infinite

def test_rolling_robust_z_normal():
    rz = RollingRobustZ(window=100)
    # feed 20 items to ensure n >= 8
    data = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0] * 2
    for x in data:
        rz.update(x)

    # median approx 5.5, MAD approx 2.5
    # z(10) approx (10-5.5)/(1.4826*2.5) = 4.5/3.7 = 1.2

    z = rz.z(10.0)
    assert 1.1 < z < 1.3

def test_processor_spread_z():
    proc = OrderFlowDataProcessor(
        symbol="BTCUSDT",
        specs=MockSpecs(),
        config=MockConfig()
    )

    # Simulate ticks to build spread history (need >= 8)
    for i in range(20):
        t = Tick(
            ts=1000 + i*100,
            bid=99.95,
            ask=100.05,
            last=100.0,
            volume=1.0,
            flags=0
        )
        proc._bucket_state.best_bid = 99.95
        proc._bucket_state.best_ask = 100.05

        proc._process_tick(t)

    st = proc._bucket_state
    assert abs(st.spread_bps - 10.0) < 1e-9
    assert abs(st.spread_bps_z) < 0.1

    # Spike spread to 20 bps
    proc._bucket_state.best_bid = 99.90
    proc._bucket_state.best_ask = 100.10
    t_spike = Tick(ts=3000, bid=99.90, ask=100.10, last=100.0, volume=1.0, flags=0)
    proc._process_tick(t_spike)

    st = proc._bucket_state
    assert abs(st.spread_bps - 20.0) < 1e-9
    assert st.spread_bps_z > 2.0

def test_processor_churn():
    proc = OrderFlowDataProcessor(
        symbol="BTCUSDT",
        specs=MockSpecs(),
        config=MockConfig()
    )
    proc.l2_engine = MagicMock() # Mock engine to avoid TypeError

    # Warmup
    proc._process_book({"snapshot": MagicMock(), "ts_ms": 1000})

    # Feed constant 10Hz updates (dt=100ms)
    for i in range(20):
        proc._process_book({"snapshot": MagicMock(), "ts_ms": 1100 + i*100})

    st = proc._bucket_state
    assert abs(st.book_churn_hz - 10.0) < 0.1
    # Variance is 0, so Z is 0 (or huge if noise? no, 0 variance = 0 MAD).
    # Wait, 1.4826*0 + eps -> denom eps. (x-med)/eps.
    # If perfect 10.0, x=10.0, med=10.0 => 0/eps = 0.
    assert abs(st.book_churn_z) < 0.1

    # Speed up to 50Hz (dt=20ms)
    # last was 3000. target 3020.
    proc._process_book({"snapshot": MagicMock(), "ts_ms": 3020})
    st = proc._bucket_state
    assert abs(st.book_churn_hz - 50.0) < 0.1
    assert st.book_churn_z > 2.0

def test_processor_ofi():
    proc = OrderFlowDataProcessor(
        symbol="BTCUSDT",
        specs=MockSpecs(),
        config=MockConfig()
    )

    # Mock L3 stats
    l3_stats = MagicMock()
    l3_stats.taker_buy_qty = 100.0
    l3_stats.taker_sell_qty = 50.0
    # OFI = +50

    # Warmup with constant OFI=+50
    for _ in range(20):
        proc._update_ofi(l3_stats)

    st = proc._bucket_state
    assert st.ofi_val == 50.0
    assert abs(st.ofi_z) < 0.1

    # Spike OFI = -200 (Sell heavy)
    l3_stats_sell = MagicMock()
    l3_stats_sell.taker_buy_qty = 0.0
    l3_stats_sell.taker_sell_qty = 200.0

    proc._update_ofi(l3_stats_sell)
    # Should be negative Z
    assert st.ofi_z < -2.0

def test_processor_recency():
    proc = OrderFlowDataProcessor(
        symbol="BTCUSDT",
        specs=MockSpecs(),
        config=MockConfig()
    )

    # 1. Init state: last_iceberg_ts = 0 (default)
    # Check context build - should use default -1 or huge age?
    # logic: age = now - last if last > 0 else -1
    ctx = proc.build_signal_ctx()
    assert ctx.iceberg_age_ms == -1

    # 2. Simulate Iceberg detection (update state directly as handler would)
    proc._bucket_state.last_iceberg_ts = 1000

    # 3. Build context at now=2000
    # We need to mock time or ensure build_signal_ctx uses state ts
    # build_signal_ctx uses st.ts or now
    proc._bucket_state.ts = 2000
    ctx = proc.build_signal_ctx()

    # age = 2000 - 1000 = 1000
    assert ctx.iceberg_age_ms == 1000

    # 4. Advance time
    proc._bucket_state.ts = 5000
    ctx = proc.build_signal_ctx()
    assert ctx.iceberg_age_ms == 4000
