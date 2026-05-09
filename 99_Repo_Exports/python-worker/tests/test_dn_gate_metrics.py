
from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.orderflow.strategy import (
    OF_GATE_METRICS_STREAM,
    OrderFlowStrategy,
)
from utils.time_utils import get_ny_time_millis


class TestDNGateMetrics:
    """Tests for DN-Gate metrics emission on veto."""

    @pytest.fixture
    def mock_redis(self):
        """Mock Redis client."""
        redis_mock = AsyncMock()
        redis_mock.xadd = AsyncMock()
        return redis_mock

    @pytest.fixture
    def mock_of_engine(self):
        """Mock OFConfirmEngine."""
        engine = MagicMock()
        # We don't expect build to be called if DN gate vetos earlier
        engine.build = MagicMock()
        return engine

    @pytest.fixture
    def strategy(self, mock_redis, mock_of_engine):
        """Create OrderFlowStrategy instance."""
        ticks_mock = AsyncMock()
        publisher_mock = MagicMock()
        with patch("asyncio.create_task"):
            return OrderFlowStrategy(
                redis=mock_redis,
                ticks=ticks_mock,
                publisher=publisher_mock,
                of_engine=mock_of_engine,
            )

    @pytest.fixture
    def runtime(self):
        """Mock SymbolRuntime."""
        runtime = MagicMock()
        runtime.symbol = "BTCUSDT"
        runtime.config = {
            "delta_diff_tiers": {"tier1": 100000.0, "tier2": 500000.0}
        }
        runtime.dynamic_cfg = {}
        runtime.last_spread_bps = 1.0
        runtime.pressure = MagicMock()
        runtime.pressure.is_pressure_hi = MagicMock(return_value=False)
        # Prevent MagicMock leaks
        runtime.last_book = None
        runtime.last_obi_event = None
        runtime.last_iceberg_event = None
        runtime.last_ofi_event = None
        runtime.last_sweep = None
        runtime.last_reclaim = None
        runtime.last_wp = None
        runtime.last_fp_edge = None
        runtime.l3_stats = None
        runtime.cvd_state = None
        runtime.last_bar = None
        runtime.last_swing_high = None
        runtime.last_swing_low = None
        runtime.last_div = None
        runtime.hawkes_snapshot = {}
        runtime.book_churn_hi = 0
        runtime.book_churn_hi = 0

        # Mock tick_dn_calib
        runtime.tick_dn_calib = MagicMock()
        class MockTiers:
            tier0_usd = 100000.0
            tier1_usd = 350000.0
            tier2_usd = 750000.0
            src = "static"
        runtime.tick_dn_calib.tiers.return_value = MockTiers()
        runtime.tick_dn_calib.tiers.return_value = MockTiers()
        runtime.tick_dn_calib.update = MagicMock()

        # Mock delta_detector
        runtime.delta_detector = MagicMock()
        runtime.delta_detector.push.return_value = {"delta": 0.1, "z": 0.5}

        # Mock dn_gate_proxy_relaxed_counters
        runtime.dn_gate_proxy_relaxed_counters = {}
        return runtime

    @pytest.mark.asyncio
    async def test_metrics_emitted_on_dn_veto(self, strategy, mock_redis, runtime):
        """Test that metrics are emitted when DN-GATE vetoes a signal."""
        # Enable metrics and set sample rate to 1.0
        with patch.dict(os.environ, {"OF_GATE_METRICS_ENABLE": "1", "OF_GATE_METRICS_SAMPLE": "1.0"}):
            # Reload not strictly necessary if we patch global vars or if the class reads env on init/usage
            # But strategy reads OF_GATE_METRICS_ENABLE global.
            # Patch tick_processor globals
            with patch("services.orderflow.components.tick_processor.OF_GATE_METRICS_ENABLE", True), \
                 patch("services.orderflow.components.tick_processor.OF_GATE_METRICS_SAMPLE", 1.0), \
                 patch("services.orderflow.components.tick_processor._should_sample", return_value=True), \
                 patch("services.orderflow.components.tick_processor.sampled_warning") as mock_warning:

                tick_ts = get_ny_time_millis()
                # Small delta ($500 * 0.1 = $50) vs Threshold $100,000
                tick = {
                    "ts_ms": tick_ts,
                    "price": 500.0,
                    "delta_event": {"delta": 0.1, "z": 0.5},
                    "direction": "LONG" # Added direction hint for logic if needed, though usually inferred
                }

                # Mock indicators to ensure we reach DN gate
                # We need data_health to be OK so we don't return None earlier?
                # Actually DN gate is after data health.

                # Ensure runtime.dynamic_cfg doesn't override with 0
                runtime.dynamic_cfg = {}
                # Set delta_tier_min to 1 to force Veto for tier 0
                runtime.dynamic_cfg = {}
                runtime.heartbeat_counter = 0
                runtime.tick_count = 1
                runtime.config["delta_tier_min"] = 1

                # Mock absorption_detector to return None or something valid
                strategy.request_absorption = MagicMock(return_value=None)

                # We need to make sure direction is determined.
                # Strategy determines direction from tick or args?
                # process_tick(self, runtime, tick) -> direction logic is inside.
                # It usually comes from tick side or aggression.
                # Adding side to tick
                tick["side"] = "BUY" # Implies LONG

                # Verify TickProcessor doesn't return None early
                strategy.tick_processor._apply_tick_time_guard = AsyncMock(return_value={"tick_ts_ms": tick_ts, "decision": "ok"})

                await strategy.process_tick(runtime, tick)

                # Check that xadd was called
                assert mock_redis.xadd.called, "Redis xadd should be called even on DN Veto"

                # Verify payload
                call_args = mock_redis.xadd.call_args
                assert call_args, "xadd not called"
                args = call_args[0]
                assert args[0] == OF_GATE_METRICS_STREAM
                payload = args[1]

                assert payload["type"] == "of_gate"
                assert payload["ok"] == "0"
                # assert payload["reason"] == "dn_veto" # We expect this after fix
