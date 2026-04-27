#!/usr/bin/env python3
"""
Integration test for UnifiedSignalPipeline in BaseOrderFlowHandler.
"""

import os
import sys
from unittest.mock import Mock, patch
from types import SimpleNamespace

# Add the python-worker directory to the path
sys.path.insert(0, os.path.dirname(__file__))

def test_unified_pipeline_integration():
    """Test that BaseOrderFlowHandler can be created with UnifiedSignalPipeline."""
    print("Testing UnifiedSignalPipeline integration with BaseOrderFlowHandler...")

    # We patch InitializationManager logic to avoid real Redis connections
    with patch('redis.Redis.from_url') as mock_redis_from_url, \
         patch('handlers.initialization_manager.InitializationManager._test_redis_connection'), \
         patch('handlers.base_orderflow_handler.get_config') as mock_config:

        # Setup mocks
        mock_redis_from_url.return_value = Mock()
        
        # Use SimpleNamespace instead of Mock for config to avoid int(Mock()) errors
        config = SimpleNamespace(
            delta_bucket_ms=1000,
            delta_window_ticks=100,
            main_z_threshold=2.0,
            breakout_z_threshold=3.0,
            obi_threshold=0.5,
            symbol="BTCUSDT",
            max_fail_retries=3,
            wall_hist_m=5,
            l2_stale_ms=2000,
            l2_skew_tick_thr_ms=5000,
            claim_min_idle_ms=60000,
            claim_count=100,
            claim_interval_ms=30000,
            max_zero_buckets=10,
            imbalance_min=0.20,
            min_trades_breakout=20,
            burst_ratio_min=1.6,
            fano_min=1.5,
            flip_ratio_max=0.70,
            use_env=True
        )
        mock_config.return_value = config

        try:
            from handlers.base_orderflow_handler import BaseOrderFlowHandler
            from handlers.handler_dependencies import HandlerDependencies

            # Create concrete subclass for testing
            class MockHandler(BaseOrderFlowHandler):
                def _get_symbol_specs(self):
                    return SimpleNamespace(
                        tick_size=0.01,
                        contract_size=1.0,
                        min_notional=10.0,
                        price_precision=2,
                        qty_precision=3
                    )
                def _process_tick(self, tick): pass
                def _process_book(self, book): pass

            # Create mock dependencies
            deps = HandlerDependencies(
                regime_service=(Mock(), Mock()), 
                scoring_engine=(Mock(), Mock()),
            )

            # Create handler
            os.environ["L2_MAX_AGE_MS"] = "5000"
            os.environ["REDIS_URL"] = "redis://localhost:6379/0"
            
            handler = MockHandler(symbol="BTCUSDT", dependencies=deps)

            # Check that _unified_pipeline attribute exists
            assert hasattr(handler, '_unified_pipeline'), "Handler should have _unified_pipeline attribute"
            
            print("✅ MockHandler created successfully with HandlerDependencies")

        except Exception as e:
            print(f"❌ Integration test failed: {e}")
            import traceback
            traceback.print_exc()
            raise e

if __name__ == "__main__":
    test_unified_pipeline_integration()
    print("\n🎉 Integration test passed!")
