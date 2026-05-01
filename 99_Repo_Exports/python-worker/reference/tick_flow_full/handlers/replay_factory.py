from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""
Replay factory: creates an adapter that can process ctx payloads and capture outbox signals.

ВАЖНО:
  Проект-специфичное место.
  Здесь вы подставляете реальный pipeline/handler, но dependencies заменяете на in-memory:
    - outbox -> OutboxCapture
    - emitter -> UnifiedSignalEmitter(outbox=OutboxCapture, ...)
    - redis providers -> None / fake / frozen snapshots (если нужно)

Минимальный результат: adapter.process_ctx(dict) вызывает pipeline.process(ctx_obj)

Реальная интеграция с CryptoOrderFlowHandler с mocked dependencies для replay.
"""

from types import SimpleNamespace
from typing import Any, Dict, Optional
import os
import time
import logging

from replay.outbox_capture import OutboxCapture
from .crypto_orderflow_handler import CryptoOrderFlowHandler
from core.instrument_config import get_config, SymbolSpecs


class FakeRedis:
    """Fake Redis that returns cached data from ctx"""
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ex=None):
        self.data[key] = value

    def xadd(self, stream, data):
        # No-op for replay
        pass


class CryptoOrderFlowReplayHandler(CryptoOrderFlowHandler):
    """
    Subclass of CryptoOrderFlowHandler that implements required abstract methods for replay.
    """

    def _get_symbol_specs(self) -> SymbolSpecs:
        # Return mock symbol specs for replay
        return SymbolSpecs(
            symbol=self.symbol,
            contract_size=1.0,
            pip_value=0.01,
            lot_step=0.001,
            min_lot=0.001,
            max_lot=1000.0,
            tick_value=0.01,
            point_value=0.01,
            price_decimals=2,
            volume_decimals=3
        )


class CryptoOrderFlowReplayAdapter:
    """
    Real adapter for CryptoOrderFlowHandler replay.

    Uses real scoring_engine, regime_service, etc. but replaces external dependencies
    (Redis) with fakes that work from ctx fields and cached data.
    """

    def __init__(self) -> None:
        self.outbox = OutboxCapture()
        self.logger = logging.getLogger("CryptoOrderFlowReplayAdapter")

        # Create fake Redis to avoid external dependencies
        fake_redis = FakeRedis()

        # Get a sample symbol for config (we'll override per ctx)
        sample_symbol = "BTCUSDT"

        # Create handler with minimal config
        config = get_config(sample_symbol, use_env=True)

        # Create handler using our replay subclass (without calling __init__ yet)
        # We'll manually call the necessary initialization steps

        # Create the handler instance manually to avoid full initialization
        self.handler = CryptoOrderFlowReplayHandler.__new__(CryptoOrderFlowReplayHandler)

        # Set basic attributes
        self.handler.symbol = sample_symbol
        self.handler.config = config

        # Override Redis before any initialization
        self.handler.redis = fake_redis
        self.handler.redis_ticks = fake_redis

        # Override the initialization manager's Redis setup
        class FakeInitManager:
            def __init__(self, handler):
                self.handler = handler

            def _init_redis_config(self, redis_url_main: str, redis_url_ticks: str) -> None:
                # Skip real Redis connection, use our fake
                self.handler.redis = fake_redis
                self.handler.redis_ticks = fake_redis

            def initialize_all(self, symbol, config, local_calibration, unified_pipeline):
                # Only call the Redis init, skip other complex initialization
                self._init_redis_config("fake", "fake")

        self.handler._init_manager = FakeInitManager(self.handler)

        # Call minimal initialization
        self.handler._initialize_symbol_specs()

        # Override outbox and emitter after initialization
        self.handler.outbox = self.outbox

        # Override emitter to use our outbox
        from handlers.emitter.unified_signal_emitter import UnifiedSignalEmitter, _NoopMetrics
        self.handler._emitter = UnifiedSignalEmitter(
            outbox=self.outbox,
            logger=self.logger,
            metrics=_NoopMetrics()
        )

        # Mock HTF provider - will work from ctx fields
        class FakeHTFProvider:
            def get_levels(self, symbol):
                # Return None - handler should fall back to ctx fields
                return None

        self.handler._htf_provider = FakeHTFProvider()

        # Set handler to use ctx-based time
        self.handler.now_ms = lambda: get_ny_time_millis()

    def process_ctx(self, ctx_payload: Dict[str, Any]) -> None:
        """
        Process ctx payload through real CryptoOrderFlowHandler.

        Dependencies work from ctx fields, external providers are mocked.
        """
        try:
            # Convert dict to SimpleNamespace for compatibility
            ctx = SimpleNamespace(**ctx_payload)

            # Override symbol if present in ctx
            if hasattr(ctx, 'symbol') and ctx.symbol:
                # Update handler config for this symbol if needed
                try:
                    config = get_config(ctx.symbol, use_env=True)
                    self.handler.config = config
                    self.handler.symbol = ctx.symbol
                except Exception:
                    pass  # Keep existing config

            # Process through the real handler pipeline
            self.handler._generate_signals(ctx)

        except Exception as e:
            self.logger.warning(f"Failed to process ctx in replay: {e}")
            # Continue - don't break replay on individual errors

    def process_tick(self, tick_payload: Dict[str, Any]) -> None:
        """
        Optional tick processing for replay.
        """
        try:
            # Convert to tick format expected by handler
            tick = SimpleNamespace(**tick_payload)
            # Call handler's tick processing if available
            if hasattr(self.handler, 'process_tick'):
                self.handler.process_tick(tick)
        except Exception as e:
            self.logger.warning(f"Failed to process tick in replay: {e}")


def create_adapter() -> Any:
    """
    Entry point for tools/tests: python_worker.handlers.replay_factory:create_adapter

    Returns real CryptoOrderFlowHandler adapter with mocked external dependencies
    for record&replay testing.
    """
    return CryptoOrderFlowReplayAdapter()
