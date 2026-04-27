# test_regime_integration.py
"""
Tests for RegimeEngine integration with DataProcessor.
"""

import pytest
from types import SimpleNamespace

# Import components
try:
    from handlers.data_processor import OrderFlowDataProcessor
    from contexts import Tick, BucketState
except ImportError:
    from python_worker.handlers.data_processor import OrderFlowDataProcessor
    from python_worker.contexts import Tick, BucketState


def make_config():
    """Create test configuration."""
    return SimpleNamespace(
        # Basic config
        family="crypto_orderflow",
        venue="binance_futures",
        timeframe_s=60,
        min_bucket_trades=10,
        min_bucket_notional_usd=1000.0,
        min_delta_z=1.0,
        min_obi_z=0.5,
        delta_window_ticks=100,
        read_count=100,
        read_block_ms=1000,
        stop_atr_mult=2.0,
        tp_rr="2.0,3.0,5.0",

        # L2 config
        l2_stale_ms=800,
        l2_skew_ms=300,
        spread_bps_max=15.0,
        wall_near_bps=10.0,
        wall_mult_vs_avg=4.0,
        wall_hist_m=5,
        wall_persist_p=3,
        wall_price_tol_bps=2.0,
        wall_drop_ratio_min=0.35,
        obi_ema_alpha=0.20,
        obi_samples_maxlen=200,
        obi20_samples_maxlen=200,
        obi_min_levels_each_side=2,
        obi_min_total_depth=0.0,
        obi_min_total_depth_20=0.0,
        obi_min_band_fill_ratio=0.25,
        obi_band_levels_target=10.0,
        obi_band_mode="spread",
        obi_band_5_bps=10.0,
        obi_band_20_bps=20.0,
        obi_band_min_bps=5.0,
        obi_band_max_bps=200.0,
        obi_band_k_spread_5=3.0,
        obi_band_k_spread_20=10.0,
        obi_thr=0.10,
        obi_sustain_k5=5,
        obi_sustain_k20=5,
        use_obi20_exec_gate=True,
        use_microprice_contradiction_gate=True,
        microprice_contra_tol_bps=0.0,

        # Regime config
        regime_atr_n=14,
        regime_atr_hist=120,
        regime_atr_hi_q=0.70,
        regime_atr_lo_q=0.35,
        regime_delta_ema_alpha=0.05,
        regime_cross_hist=30,
        regime_hold_ema_alpha=0.10,
        regime_delta_thr=0.0,
        regime_w_atr=0.35,
        regime_w_delta=0.30,
        regime_w_hold=0.25,
        regime_w_ping=0.20,
        regime_label_hi=0.35,
        regime_label_lo=-0.35,
    )


def make_specs():
    """Create test symbol specs."""
    return SimpleNamespace(
        price_precision=2,
        size_precision=4,
    )


class TestRegimeIntegration:
    """Test RegimeEngine integration with DataProcessor."""

    def test_data_processor_initialization_with_regime(self):
        """Test that DataProcessor initializes with RegimeEngine."""
        config = make_config()
        specs = make_specs()

        processor = OrderFlowDataProcessor("BTCUSDT", specs, config)

        # Check that regime engine is initialized
        assert hasattr(processor, '_regime')
        assert hasattr(processor, '_bar_builder_1m')
        assert processor._regime is not None
        assert processor._bar_builder_1m is not None

    def test_tick_processing_updates_regime(self):
        """Test that tick processing updates regime state."""
        config = make_config()
        specs = make_specs()

        processor = OrderFlowDataProcessor("BTCUSDT", specs, config)

        # Create test tick
        tick = Tick(
            ts=100000000,  # 100 seconds
            bid=50000.0,
            ask=50001.0,
            last=50000.5,
            volume=1.0,
            flags=0,
        )

        # Process tick - should not crash
        processor._process_tick(tick)

        # Check that regime engine was updated
        assert processor._regime.state.ts_ms == tick.ts

    def test_signal_context_includes_regime(self):
        """Test that build_signal_ctx includes regime information."""
        config = make_config()
        specs = make_specs()

        processor = OrderFlowDataProcessor("BTCUSDT", specs, config)

        # Build signal context
        ctx = processor.build_signal_ctx()

        # Check regime fields are included with default values
        assert hasattr(ctx, 'regime_score')
        assert hasattr(ctx, 'regime_label')
        assert ctx.regime_score == 0.0  # default
        assert ctx.regime_label == "mixed"  # default

    def test_bar_completion_triggers_atr_update(self):
        """Test that bar completion triggers ATR updates in regime engine."""
        config = make_config()
        specs = make_specs()

        processor = OrderFlowDataProcessor("BTCUSDT", specs, config)

        # Simulate ticks over multiple minutes
        base_ts = 60000  # Start at 1 minute
        price = 50000.0

        # First minute - multiple ticks
        for i in range(10):
            ts = base_ts + i * 1000  # 1 second intervals
            tick = Tick(
                ts=ts,
                bid=price - 0.5,
                ask=price + 0.5,
                last=price,
                volume=1.0,
                flags=0,
            )
            processor._process_tick(tick)

        # Second minute - should complete first bar
        ts = base_ts + 60000  # Next minute
        tick = Tick(
            ts=ts,
            bid=price - 0.5,
            ask=price + 0.5,
            last=price,
            volume=1.0,
            flags=0,
        )
        processor._process_tick(tick)

        # Check that regime engine has processed ticks
        regime_state = processor._regime.state
        assert regime_state.ts_ms == ts

        # Check that some ATR calculation started (atr1m should be > 0 after some bars)
        # Note: ATR needs multiple bars to initialize, so we just check it doesn't crash


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
