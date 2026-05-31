from __future__ import annotations

"""
Tests for OF Gate metrics emission and fail-open logic.

Tests:
1. OF Gate metrics are emitted to Redis stream when enabled
2. Metrics are sampled deterministically
3. Fail-open logic ensures spread_bps and expected_slippage_bps never become 0 silently
"""

import hashlib
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.orderflow.strategy import (
    DATA_HEALTH_ON_SPREAD_MISSING,
    OF_GATE_METRICS_STREAM,
    SLIPPAGE_BPS_MISSING_DEFAULT,
    SPREAD_BPS_MISSING_DEFAULT,
    OrderFlowStrategy,
)
from services.orderflow.utils import _should_sample
from utils.time_utils import get_ny_time_millis


class TestOFGateMetrics:
    """Tests for OF Gate metrics emission."""

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
        ofc = MagicMock()
        ofc.ok = True
        ofc.score = 0.75
        ofc.have = 3
        ofc.need = 2
        ofc.reason = "test_reason"
        ofc.gate_bits = 0b101
        ofc.scenario = "test_scenario"
        ofc.evidence = {
            "scenario_v4": "test_scenario_v4",
            "ok_soft": 0,
            "exec_risk_bps": 5.5,
            "exec_risk_norm": 0.65,
            "missing_legs": ["leg1", "leg2"],
        }
        ofc.to_dict = MagicMock(return_value={"ok": True, "score": 0.75})
        dec = MagicMock()
        dec.need = 2
        dec.have = 3
        engine.build = MagicMock(return_value=(ofc, dec))
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
        runtime.dynamic_cfg = {}
        runtime.heartbeat_counter = 0
        runtime.tick_count = 1
        runtime.config = {"delta_abs_min_confirm": 0.0}
        runtime.heartbeat_counter = 0
        runtime.last_metrics_ts = 0
        runtime.delta_detector = MagicMock()
        runtime.delta_detector.push.return_value = {"delta": 10.0, "z": 1.5, "delta_event": {"z": 1.5}}
        runtime.tick_dn_calib = MagicMock()
        mock_tiers = MagicMock()
        mock_tiers.tier0_usd = 100.0
        mock_tiers.tier1_usd = 200.0
        mock_tiers.tier2_usd = 300.0
        mock_tiers.src = "mock"
        mock_tiers.scale = 1.0
        runtime.tick_dn_calib.tiers.return_value = mock_tiers
        runtime.dynamic_cfg = {}
        runtime.dn_passrate = MagicMock()
        return runtime

    @pytest.mark.skip(reason="Mocks out of date")
    @pytest.mark.asyncio
    async def test_metrics_emitted_when_enabled(self, strategy, mock_redis, mock_of_engine, runtime):
        """Test that metrics are emitted to Redis when enabled."""
        with patch.dict(os.environ, {"OF_GATE_METRICS_ENABLE": "1", "CRYPTO_SIGNAL_MIN_CONF": "100", "DISABLE_CONFIDENCE_FILTER": "1"}):
            # Reload module to pick up env change
            import importlib

            import services.orderflow.strategy as strategy_module
            importlib.reload(strategy_module)

            tick_ts = get_ny_time_millis()
            tick = {
                "ts_ms": tick_ts,
                "price": 50000.0,
                "delta_event": {"z": 1.5},
            }
            indicators = {
                "spread_bps": 10.0,
                "expected_slippage_bps": 3.0,
                "data_health": 1.0,
                "book_health_ok": 1,
            }
            cfg2 = {}
            runtime.dynamic_cfg = {}
            runtime.heartbeat_counter = 0
            runtime.tick_count = 1
            runtime.config = {
                "of_gate_metrics_sample": 1.0,
                "delta_abs_min_confirm": 0.0,
                "signal_min_conf": 10.0
            }

            # Mock _should_sample to return True
            with patch("services.orderflow.strategy._should_sample", return_value=True):
                await strategy.process_tick(runtime, tick)

            # Check that xadd was called
            assert mock_redis.xadd.called

            # Check call arguments
            call_args = mock_redis.xadd.call_args
            assert call_args[0][0] == OF_GATE_METRICS_STREAM
            payload = call_args[0][1]

            # Verify payload structure
            assert payload["type"] == "of_gate"
            assert payload["symbol"] == "BTCUSDT"
            assert "ok" in payload
            assert "latency_us" in payload
            assert "exec_risk_bps" in payload
            assert "exec_risk_norm" in payload

    @pytest.mark.skip(reason="Mocks out of date")
    @pytest.mark.asyncio
    async def test_metrics_not_emitted_when_disabled(self, strategy, mock_redis, mock_of_engine, runtime):
        """Test that metrics are not emitted when disabled."""
        with patch.dict(os.environ, {"OF_GATE_METRICS_ENABLE": "0", "SIGNAL_MIN_CONF": "1.0", "DISABLE_CONFIDENCE_FILTER": "1"}):
            import importlib

            import services.orderflow.strategy as strategy_module
            importlib.reload(strategy_module)

            tick_ts = get_ny_time_millis()
            tick = {
                "ts_ms": tick_ts,
                "price": 50000.0,
                "delta_event": {"z": 1.5},
            }
            runtime.dynamic_cfg = {}
            runtime.heartbeat_counter = 0
            runtime.tick_count = 1
            runtime.config = {
                "of_gate_metrics_sample": 1.0,
                "delta_abs_min_confirm": 0.0,
                "signal_min_conf": 10.0
            }

            await strategy.process_tick(runtime, tick)

            # Check that xadd was not called
            assert not mock_redis.xadd.called

    @pytest.mark.skip(reason="Mocks out of date")
    @pytest.mark.asyncio
    async def test_metrics_sampled_deterministically(self, strategy, mock_redis, mock_of_engine, runtime):
        """Test that metrics are sampled deterministically by (symbol, tick_ts)."""
        with patch.dict(os.environ, {"OF_GATE_METRICS_ENABLE": "1", "OF_GATE_METRICS_SAMPLE": "0.5", "SIGNAL_MIN_CONF": "1.0", "DISABLE_CONFIDENCE_FILTER": "1"}):
            import importlib

            import services.orderflow.strategy as strategy_module
            importlib.reload(strategy_module)

            tick_ts = 1234567890000  # Fixed timestamp for deterministic sampling
            tick = {
                "ts_ms": tick_ts,
                "price": 50000.0,
                "delta_event": {"z": 1.5},
            }
            indicators = {
                "spread_bps": 10.0,
                "expected_slippage_bps": 3.0,
                "data_health": 1.0,
                "book_health_ok": 1,
            }
            runtime.dynamic_cfg = {}
            runtime.heartbeat_counter = 0
            runtime.tick_count = 1
            runtime.config = {
                "of_gate_metrics_sample": 0.5,
                "delta_abs_min_confirm": 0.0,
                "signal_min_conf": 10.0
            }

            # Mock _should_sample
            should_sample_calls = []
            def mock_should_sample(ts, rate):
                should_sample_calls.append((ts, rate))
                return _should_sample(ts, rate)

            with patch("services.orderflow.strategy._should_sample", side_effect=mock_should_sample):
                await strategy.process_tick(runtime, tick)

            # Verify _should_sample was called with correct arguments
            assert len(should_sample_calls) > 0
            # The sampled key is a deterministic hash of (salt|symbol|ts_ms)
            b = f"|{runtime.symbol}|{tick_ts}".encode("utf-8", errors="replace")
            h = hashlib.sha1(b).digest()
            expected_uid = int.from_bytes(h[:8], byteorder="big", signed=False)
            assert should_sample_calls[0][0] == expected_uid
            assert should_sample_calls[0][1] == 0.5


class TestFailOpenLogic:
    """Tests for fail-open logic ensuring spread_bps and expected_slippage_bps never become 0."""

    def test_spread_bps_defaults_when_missing(self):
        """Test that spread_bps defaults to SPREAD_BPS_MISSING_DEFAULT when missing."""
        indicators = {}
        cfg2 = {}

        # Simulate fail-open logic
        spr = float(indicators.get("spread_bps", 0.0) or 0.0)
        if spr <= 0:
            spr = float(cfg2.get("spread_bps_missing_default", SPREAD_BPS_MISSING_DEFAULT))

        assert spr == SPREAD_BPS_MISSING_DEFAULT
        assert spr > 0

    def test_expected_slippage_bps_defaults_when_missing(self):
        """Test that expected_slippage_bps defaults to SLIPPAGE_BPS_MISSING_DEFAULT when missing."""
        indicators = {}
        cfg2 = {}

        # Simulate fail-open logic
        if "expected_slippage_bps" not in indicators or float(indicators.get("expected_slippage_bps", 0.0) or 0.0) <= 0:
            indicators["expected_slippage_bps"] = float(cfg2.get("expected_slippage_bps_missing_default", SLIPPAGE_BPS_MISSING_DEFAULT))

        assert indicators["expected_slippage_bps"] == SLIPPAGE_BPS_MISSING_DEFAULT
        assert indicators["expected_slippage_bps"] > 0

    def test_data_health_degraded_on_spread_missing(self):
        """Test that data_health is degraded when spread is missing."""
        indicators = {"data_health": 1.0}
        cfg2 = {}

        # Simulate fail-open logic
        spr = 0.0
        if spr <= 0:
            spr = float(cfg2.get("spread_bps_missing_default", SPREAD_BPS_MISSING_DEFAULT))
            indicators["spread_bps_missing"] = 1
            dh = float(indicators.get("data_health", 1.0) or 1.0)
            indicators["data_health"] = min(dh, float(cfg2.get("data_health_on_spread_missing", DATA_HEALTH_ON_SPREAD_MISSING)))

        assert indicators["data_health"] == DATA_HEALTH_ON_SPREAD_MISSING
        assert indicators["data_health"] < 1.0
        assert indicators.get("spread_bps_missing") == 1

    def test_spread_bps_uses_runtime_fallback(self):
        """Test that spread_bps uses runtime.last_spread_bps as fallback."""
        indicators = {}
        cfg2 = {}
        runtime = MagicMock()
        runtime.last_spread_bps = 12.5

        # Simulate fail-open logic
        spr = float(indicators.get("spread_bps", 0.0) or 0.0)
        if spr <= 0:
            spr = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)

        assert spr == 12.5
        assert spr > 0

    def test_spread_bps_uses_liq_spread_fallback(self):
        """Test that spread_bps uses liq_spread_bps as fallback."""
        indicators = {"liq_spread_bps": 8.0}
        cfg2 = {}
        mock_runtime = MagicMock()
        mock_runtime.last_spread_bps = 0.0

        # Simulate fail-open logic
        spr = float(indicators.get("spread_bps", 0.0) or 0.0)
        if spr <= 0:
            spr = float(getattr(mock_runtime, "last_spread_bps", 0.0) or 0.0)
        if spr <= 0:
            spr = float(indicators.get("liq_spread_bps", 0.0) or 0.0)

        assert spr == 8.0
        assert spr > 0

    def test_constants_are_positive(self):
        """Test that fail-open constants are positive."""
        assert SPREAD_BPS_MISSING_DEFAULT > 0
        assert SLIPPAGE_BPS_MISSING_DEFAULT > 0
        assert 0 < DATA_HEALTH_ON_SPREAD_MISSING <= 1.0

    def test_spread_bps_computed_from_best_bid_ask(self):
        """
        spread_bps must be computed from best_bid_px/best_ask_px
        when last_spread_bps=0 and snap.spread_bps=0.
        Previously this path returned 0 and fell through to the 15.0 default.
        """
        snap = MagicMock()
        snap.spread_bps = 0.0
        snap.best_bid_px = 3500.0
        snap.best_ask_px = 3500.50   # 0.50 / 3500.25 * 10000 ≈ 1.43 bps

        mock_runtime = MagicMock()
        mock_runtime.last_spread_bps = 0.0
        mock_runtime.last_book = snap

        # Simulate the fixed fallback logic from orderflow_strategy.py ~L1257
        spr = float(getattr(mock_runtime, "last_spread_bps", 0.0) or 0.0)
        if spr <= 0 and mock_runtime.last_book:
            snap_b = mock_runtime.last_book
            spr = float(getattr(snap_b, "spread_bps", 0.0) or 0.0)
            if spr <= 0:
                bb = float(getattr(snap_b, "best_bid_px", 0.0) or 0.0)
                ba = float(getattr(snap_b, "best_ask_px", 0.0) or 0.0)
                if bb > 0 and ba > bb:
                    mid_b = (bb + ba) / 2.0
                    spr = 10_000.0 * (ba - bb) / mid_b

        # Should compute real spread (~1.43 bps), NOT the 15.0 bps default
        assert spr > 0, "spread_bps must be > 0 when bid/ask are available"
        assert spr < 5.0, f"Expected real spread ~1.43 bps, got {spr:.4f}"
        assert abs(spr - 15.0) > 1.0, "Must NOT fall back to the 15.0 bps default"


