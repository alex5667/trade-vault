# test_regime_end_to_end.py
"""
End-to-end tests for Regime Engine integration.
"""

import pytest
from types import SimpleNamespace

# Import components
try:
    from handlers.data_processor import OrderFlowDataProcessor
    from handlers.signal_generator import SignalGenerator
    from contexts import Tick
except ImportError:
    from python_worker.handlers.data_processor import OrderFlowDataProcessor
    from python_worker.handlers.signal_generator import SignalGenerator
    from python_worker.contexts import Tick


def make_config():
    """Create test configuration with regime parameters."""
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

        # Signal generator config
        min_trades_breakout=20,
        burst_ratio_min=1.6,
        fano_min=1.5,
        flip_ratio_max=0.70,
        imbalance_min=0.20,
        signal_z_enter=1.5,
        signal_z_breakout=2.0,

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


class TestRegimeEndToEnd:
    """End-to-end tests for regime-aware signal generation."""

    def test_regime_integration_in_data_processor(self):
        """Test that DataProcessor integrates RegimeEngine."""
        config = make_config()
        specs = make_specs()

        processor = OrderFlowDataProcessor("BTCUSDT", specs, config)

        # Check that regime engine is properly initialized
        assert hasattr(processor, '_regime')
        assert hasattr(processor, '_bar_builder_1m')
        assert processor._regime is not None

        # Test tick processing doesn't crash
        tick = Tick(
            ts=100000000,
            bid=50000.0,
            ask=50001.0,
            last=50000.5,
            volume=1.0,
            flags=0,
        )

        processor._process_tick(tick)

        # Build signal context
        ctx = processor.build_signal_ctx()

        # Should have regime fields
        assert hasattr(ctx, 'regime_score')
        assert hasattr(ctx, 'regime_label')

    def test_regime_hard_gate_in_signal_generator(self):
        """Test that SignalGenerator applies regime-based hard gate."""
        config = make_config()
        specs = make_specs()

        processor = OrderFlowDataProcessor("BTCUSDT", specs, config)
        generator = SignalGenerator("BTCUSDT", config, None)

        # Create signal context
        ctx = processor.build_signal_ctx()
        ctx.l2_is_stale = False
        ctx.obi_20_valid = True

        # Test breakout allowed in trend regime
        ctx.regime_score = 0.6  # Trend
        ctx.regime_label = "trend"
        ctx.z_delta = 2.5  # Breakout signal (above z_breakout=2.0)
        ctx.obi_avg_20 = 0.3
        assert generator._exec_quality_ok(ctx, "buy") is True

        # Test breakout blocked in range regime
        ctx.regime_score = -0.6  # Range
        ctx.regime_label = "range"
        assert generator._exec_quality_ok(ctx, "buy") is False

        # Test mean reversion allowed in range regime
        ctx.z_delta = -1.8  # Mean reversion (above z_enter threshold)
        ctx.obi_avg_20 = -0.25
        ctx.weak_progress = True
        assert generator._exec_quality_ok(ctx, "sell") is True

        # Test mean reversion blocked in trend regime
        ctx.regime_score = 0.6  # Trend
        ctx.regime_label = "trend"
        assert generator._exec_quality_ok(ctx, "sell") is False

    def test_mixed_regime_allows_both_signals(self):
        """Test that mixed regime allows both signal types."""
        config = make_config()
        specs = make_specs()

        processor = OrderFlowDataProcessor("BTCUSDT", specs, config)
        generator = SignalGenerator("BTCUSDT", config, None)

        ctx = processor.build_signal_ctx()
        ctx.regime_score = 0.0  # Mixed
        ctx.regime_label = "mixed"
        ctx.l2_is_stale = False
        ctx.obi_20_valid = True

        # Breakout in mixed regime
        ctx.z_delta = 2.5
        ctx.obi_avg_20 = 0.3
        assert generator._exec_quality_ok(ctx, "buy") is True

        # Mean reversion in mixed regime
        ctx.z_delta = -1.8
        ctx.obi_avg_20 = -0.25
        ctx.weak_progress = True
        assert generator._exec_quality_ok(ctx, "sell") is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])